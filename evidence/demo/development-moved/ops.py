"""Deterministic packaging and a single-primary user-service scaffold."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import zipfile

import runtime
import journal

CODE = ("pipeline.py", "runtime.py", "journal.py", "ops.py", "checks.py", "lab.py")
MAIN = "from ops import main\nmain()\n"


def archive(items):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as target:
        for name, body in sorted(items.items()):
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            target.writestr(info, body)
    return buffer.getvalue()


def build(destination):
    source = Path(__file__).resolve().parent
    app = archive({**{name: (source / name).read_bytes() for name in CODE},
                   "__main__.py": MAIN.encode()})
    manifest = {"format": 1, "sha256": hashlib.sha256(app).hexdigest()}
    Path(destination).write_bytes(archive({"app.pyz": app,
                                           "manifest.json": runtime.encode(manifest)}))
    return manifest


def unpack(release):
    with zipfile.ZipFile(release) as package:
        if sorted(package.namelist()) != ["app.pyz", "manifest.json"]:
            raise ValueError("PACKAGE_LAYOUT")
        manifest = json.loads(package.read("manifest.json"))
        app = package.read("app.pyz")
    if manifest != {"format": 1, "sha256": hashlib.sha256(app).hexdigest()}:
        raise ValueError("PACKAGE_HASH")
    return app


def identity(root):
    return "local.sampling." + hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()[:16]


def agent_path(root):
    return Path.home() / "Library" / "LaunchAgents" / (identity(root) + ".plist")


def install(release, root, control):
    app = unpack(release)  # Verify before creating or changing ROOT.
    root = Path(root).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("INSTALL_TARGET_NOT_EMPTY")
    journal.init(root, control)
    (root / "app.pyz").write_bytes(app)
    return {"root": str(root), "sha256": hashlib.sha256(app).hexdigest()}


def start(root):
    root = Path(root).resolve()
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    if sys.platform == "darwin":
        config = {
            "Label": identity(root),
            "ProgramArguments": [str(Path(sys.executable).resolve()), str(root / "app.pyz"),
                                 "worker", str(root)],
            "WorkingDirectory": str(root),
            "RunAtLoad": True,
            "KeepAlive": True,
            "ThrottleInterval": 2,
            "StandardOutPath": str(control / "service.stdout"),
            "StandardErrorPath": str(control / "service.stderr"),
        }
        path = agent_path(root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(plistlib.dumps(config))
        domain = "gui/" + str(os.getuid())
        registered = subprocess.run(["launchctl", "print", domain + "/" + identity(root)],
                                    capture_output=True).returncode == 0
        if not registered:
            subprocess.run(["launchctl", "bootstrap", domain, str(path)], check=True)
        subprocess.run(["launchctl", "kickstart", domain + "/" + identity(root)], check=True)
        return {"label": identity(root), "plist": str(path)}
    # Non-macOS development hosts have no launchd; a detached supervisor gives
    # the same KeepAlive semantics through the same start/stop entry points.
    pidfile = control / "supervisor.pid"
    if pidfile.exists():
        try:
            pid = int(pidfile.read_text())
            os.kill(pid, 0)
            return {"label": identity(root), "supervisor_pid": pid, "already_running": True}
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    entry = str(Path(sys.argv[0]).resolve())
    with (control / "service.stdout").open("ab") as out, \
         (control / "service.stderr").open("ab") as err:
        child = subprocess.Popen([sys.executable, entry, "supervisor", str(root)],
                                 stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                 cwd=str(root), start_new_session=True)
    pidfile.write_text(str(child.pid))
    return {"label": identity(root), "supervisor_pid": child.pid}


def stop(root):
    root = Path(root).resolve()
    if sys.platform == "darwin":
        target = "gui/" + str(os.getuid()) + "/" + identity(root)
        subprocess.run(["launchctl", "bootout", target], capture_output=True)
        agent_path(root).unlink(missing_ok=True)
        return {"stopped": identity(root)}
    import signal
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    pidfile = control / "supervisor.pid"
    if pidfile.exists():
        try:
            # The supervisor is a session leader; this also reaches the worker
            # and any node-step children in its process group.
            os.killpg(int(pidfile.read_text()), signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    worker_pid = control / "worker.pid"
    if worker_pid.exists():
        try:
            os.kill(int(worker_pid.read_text()), signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    pidfile.unlink(missing_ok=True)
    worker_pid.unlink(missing_ok=True)
    return {"stopped": identity(root)}


def supervisor(root):
    """KeepAlive loop for hosts without launchd: restart the worker forever."""
    import time
    root = Path(root).resolve()
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    entry = str(Path(sys.argv[0]).resolve())
    (control / "supervisor.pid").write_text(str(os.getpid()))
    while True:
        with (control / "service.stderr").open("ab") as error:
            child = subprocess.Popen([sys.executable, entry, "worker", str(root)],
                                     stdout=subprocess.DEVNULL, stderr=error, cwd=str(root))
        (control / "worker.pid").write_text(str(child.pid))
        child.wait()
        (control / "worker.pid").unlink(missing_ok=True)
        time.sleep(2)


def node_step(root, node):
    import fcntl
    with (journal.replica(root, node) / "worker.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"processed": False, "busy": True}
        return {"processed": journal.replay_one(root, node)}


def worker(root):
    import time
    root = Path(root).resolve()
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    entry = str(Path(sys.argv[0]).resolve())
    children = {}
    while True:
        try:
            state = journal.failover_state(root)
        except Exception as error:
            print("failover state unreadable: %r" % (error,), file=sys.stderr, flush=True)
            time.sleep(0.5)
            continue
        if state is not None and state["phase"] != "done":
            # A takeover is pending: finish it instead of driving the old
            # instance. Every step is idempotent, so a SIGKILLed coordinator
            # simply re-runs this after the auto-start facility revives it.
            try:
                journal.failover_step(root)
            except Exception as error:
                print("failover step failed: %r" % (error,), file=sys.stderr, flush=True)
                time.sleep(0.5)
            continue
        for node, child in list(children.items()):
            if child.poll() is not None:
                del children[node]
        node = journal.current(root)
        if node not in children:
            # A stale executor paused in a public hook is left alone: it is
            # never waited on or killed, and after a takeover the new current
            # instance gets its own child while the stale one stays paused.
            with (control / (node + ".stderr")).open("ab") as error:
                child = subprocess.Popen([sys.executable, entry, "node-step", str(root), node],
                                         stdout=error, stderr=error)
            (control / (node + ".pid")).write_text(str(child.pid))
            children[node] = child
        time.sleep(0.2)


def main(argv=None):
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest="operation", required=True)
    item = subs.add_parser("build")
    item.add_argument("release")
    item = subs.add_parser("install")
    item.add_argument("release")
    item.add_argument("root")
    item.add_argument("--control", required=True)
    for name in ("start", "stop", "status", "run", "worker", "failover-status", "supervisor"):
        subs.add_parser(name).add_argument("root")
    item = subs.add_parser("promote")
    item.add_argument("root")
    item.add_argument("request_id")
    for name, argument in (("submit", "input"), ("read", "batch_id")):
        item = subs.add_parser(name)
        item.add_argument("root")
        item.add_argument(argument)
    for name in ("replay", "inspect", "node-step"):
        item = subs.add_parser(name)
        item.add_argument("root")
        item.add_argument("node", choices=("a", "b"))
        if name == "inspect":
            item.add_argument("batch_id")
    subs.add_parser("selftest")
    args = parser.parse_args(argv)
    try:
        if args.operation == "build":
            value = build(args.release)
        elif args.operation == "install":
            value = install(args.release, args.root, args.control)
        elif args.operation == "start":
            value = start(args.root)
        elif args.operation == "stop":
            value = stop(args.root)
        elif args.operation == "status":
            value = journal.status(args.root)
        elif args.operation == "failover-status":
            value = journal.failover_status(args.root)
        elif args.operation == "promote":
            value = journal.promote(args.root, args.request_id)
        elif args.operation == "submit":
            value = journal.submit(args.root, json.loads(Path(args.input).read_bytes()))
        elif args.operation == "read":
            value = journal.read(args.root, args.batch_id)
        elif args.operation == "inspect":
            value = runtime.read(journal.replica(args.root, args.node), args.batch_id)
        elif args.operation in ("replay", "node-step"):
            value = node_step(args.root, args.node)
        elif args.operation == "run":
            value = node_step(args.root, journal.current(args.root))
        elif args.operation == "worker":
            worker(args.root)
            return
        elif args.operation == "supervisor":
            supervisor(args.root)
            return
        else:
            import checks
            value = checks.verify()
    except journal.Maintenance:
        print(json.dumps({"error": "MAINTENANCE", "retryable": True}, sort_keys=True))
        raise SystemExit(75)
    except ValueError as error:
        print(json.dumps({"error": str(error)}, sort_keys=True))
        raise SystemExit(2)
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
