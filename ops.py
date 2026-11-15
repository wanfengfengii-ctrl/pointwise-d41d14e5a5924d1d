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


def stop(root):
    target = "gui/" + str(os.getuid()) + "/" + identity(root)
    subprocess.run(["launchctl", "bootout", target], capture_output=True)
    agent_path(root).unlink(missing_ok=True)
    return {"stopped": identity(root)}


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
    while True:
        node = journal.current(root)
        with (control / (node + ".stderr")).open("ab") as error:
            child = subprocess.Popen([sys.executable, entry, "node-step", str(root), node],
                                     stdout=subprocess.DEVNULL, stderr=error)
            (control / (node + ".pid")).write_text(str(child.pid))
            child.wait()
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
    for name in ("start", "stop", "status", "run", "worker"):
        subs.add_parser(name).add_argument("root")
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
        else:
            import checks
            value = checks.verify()
    except ValueError as error:
        print(json.dumps({"error": str(error)}, sort_keys=True))
        raise SystemExit(2)
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
