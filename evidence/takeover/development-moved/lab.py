"""Real installed primary/standby rehearsals: normal flow, takeover, demo."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import zipfile

import ops
import pipeline


def command(argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, capture_output=True,
                          text=True, **kwargs).stdout


def wait_for(predicate, message, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(message)


def service_pid(root):
    if sys.platform == "darwin":
        import re
        target = "gui/%d/%s" % (os.getuid(), ops.identity(root))
        result = subprocess.run(["launchctl", "print", target],
                                capture_output=True, text=True)
        match = re.search(r"\bpid = ([0-9]+)", result.stdout)
        return int(match[1]) if match else None
    control = Path(json.loads((Path(root) / "config.json").read_bytes())["control"])
    pidfile = control / "worker.pid"
    if not pidfile.exists():
        return None
    try:
        pid = int(pidfile.read_text())
        os.kill(pid, 0)
    except (ValueError, ProcessLookupError, PermissionError):
        return None
    return pid


def normal(release, directory):
    release = Path(release).resolve()
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    development = directory / "development"
    development.mkdir()
    with zipfile.ZipFile(io.BytesIO(ops.unpack(release))) as package:
        for name in ops.CODE:
            (development / name).write_bytes(package.read(name))
    rebuilt = directory / "rebuilt.zip"
    command([sys.executable, development / "ops.py", "build", rebuilt], cwd=directory)
    assert rebuilt.read_bytes() == release.read_bytes(), "NONREPRODUCIBLE_PACKAGE"
    development.rename(directory / "development-moved")
    root, control = directory / "service", directory / "control"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    evidence = {"source_moved": True}
    app = root / "app.pyz"
    installed = False

    def call(operation, *args):
        route = [] if operation == "selftest" else [root]
        return json.loads(command([sys.executable, app, operation, *route, *args],
                                  cwd=directory, env=env))

    try:
        # Installation itself is performed by the rebuilt release, not host imports.
        bootstrap = directory / "bootstrap.pyz"
        bootstrap.write_bytes(ops.unpack(rebuilt))
        command([sys.executable, bootstrap, "install", rebuilt, root,
                 "--control", control], cwd=directory, env=env)
        installed = True
        from checks import small
        first = small()
        second = {"batch_id": "second", "scale": 2, "rows": [
            {"id": "x", "rev": 1, "station": "north", "minute": 2,
             "value": 8, "deleted": False},
            {"id": "x", "rev": 2, "station": "south", "minute": 3,
             "value": -4, "deleted": False},
            {"id": "y", "rev": 1, "station": "south", "minute": 3,
             "value": 4, "deleted": False}]}
        fixtures = []
        for batch in (first, second):
            path = directory / (batch["batch_id"] + ".json")
            path.write_bytes(pipeline.encode(batch))
            fixtures.append(path)
        evidence["accepted"] = [call("submit", path) for path in fixtures]
        evidence["agent"] = call("start")
        evidence["coordinator_pid"] = wait_for(lambda: service_pid(root), "NO_AGENT_PID")
        wait_for(lambda: call("status")["replicas"]["a"] == 2, "PRIMARY_NOT_READY")
        evidence["selftest"] = call("selftest")
        evidence["primary"] = [call("read", batch["batch_id"]) for batch in (first, second)]
        evidence["standby_replay"] = [call("replay", "b"), call("replay", "b")]
        evidence["standby"] = [call("inspect", "b", batch["batch_id"])
                               for batch in (first, second)]
        assert evidence["primary"] == evidence["standby"]
        evidence["installed"] = []
        for node in ("a", "b"):
            for path in fixtures:
                evidence["installed"].append(json.loads(command(
                    [sys.executable, "-c",
                     "import sys,json; sys.path.insert(0,sys.argv[1]); import checks; "
                     "print(json.dumps(checks.installed(sys.argv[2],json.load(open(sys.argv[3])))))",
                     app, root / node, path], cwd=directory, env=env)))
        evidence["status"] = call("status")
        assert evidence["status"]["active"] == "a"
        assert evidence["status"]["replicas"] == {"a": 2, "b": 2}
        evidence["regression"] = "INSTALLED_DUAL_REPLAY_OK"
        return evidence
    finally:
        if installed:
            call("stop")
        (directory / "evidence.json").write_bytes(pipeline.encode(evidence))


def prepare(release, directory):
    """Rebuild the release from its own unpacked sources, prove byte identity,
    move that development copy away, and keep a bootstrap pyz for installs."""
    development = directory / "development"
    development.mkdir()
    with zipfile.ZipFile(io.BytesIO(ops.unpack(release))) as package:
        for name in ops.CODE:
            (development / name).write_bytes(package.read(name))
    rebuilt = directory / "rebuilt.zip"
    command([sys.executable, development / "ops.py", "build", rebuilt], cwd=directory)
    assert rebuilt.read_bytes() == release.read_bytes(), "NONREPRODUCIBLE_PACKAGE"
    development.rename(directory / "development-moved")
    bootstrap = directory / "bootstrap.pyz"
    bootstrap.write_bytes(ops.unpack(rebuilt))
    return bootstrap


def fresh_directory(path):
    directory = Path(path).resolve()
    if directory.exists() and any(directory.iterdir()):
        raise SystemExit("EVIDENCE_DIR_NOT_EMPTY: remove %s to re-run" % directory)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def batches():
    from checks import small, second_batch, third_batch
    return small(), second_batch(), third_batch()


class Harness:
    """Drives one installed service through its app.pyz and records evidence."""

    def __init__(self, directory, env, log):
        self.directory = directory
        self.env = env
        self.log = log

    def call(self, root, *args, expect=0):
        argv = [str(sys.executable), str(Path(root) / "app.pyz")]
        argv += [str(a) for a in args]
        proc = subprocess.run(argv, capture_output=True, text=True,
                              cwd=self.directory, env=self.env)
        entry = {"argv": argv, "exit": proc.returncode}
        if proc.stdout.strip():
            try:
                entry["json"] = json.loads(proc.stdout)
            except ValueError:
                entry["stdout"] = proc.stdout.strip()
        if proc.stderr.strip():
            entry["stderr"] = proc.stderr.strip()[-400:]
        self.log.append(entry)
        assert proc.returncode == expect, entry
        return entry.get("json")

    def installed(self, app, node_root, fixture):
        argv = [sys.executable, "-c",
                "import sys,json; sys.path.insert(0,sys.argv[1]); import checks; "
                "print(json.dumps(checks.installed(sys.argv[2],json.load(open(sys.argv[3])))))",
                app, node_root, fixture]
        out = command([str(a) for a in argv], cwd=self.directory, env=self.env)
        value = json.loads(out)
        self.log.append({"argv": [str(a) for a in argv], "exit": 0, "json": value})
        return value

    def checks_failover(self, app):
        argv = [sys.executable, "-c",
                "import sys,json; sys.path.insert(0,sys.argv[1]); import checks; "
                "print(json.dumps(checks.failover()))", app]
        value = json.loads(command(argv, cwd=self.directory, env=self.env))
        self.log.append({"argv": [str(a) for a in argv], "exit": 0,
                         "json": {"regression": value["regression"]}})
        return value


def install_root(harness, bootstrap, rebuilt, root, control):
    harness.log.append({"argv": [str(sys.executable), str(bootstrap), "install",
                                 str(rebuilt), str(root), "--control", str(control)],
                        "exit": 0,
                        "json": json.loads(command(
                            [sys.executable, bootstrap, "install", rebuilt, root,
                             "--control", control], cwd=harness.directory,
                            env=harness.env))})


def takeover(release, directory):
    """Go-live regression on installed artifacts: paused-old-primary takeover,
    coordinator SIGKILL at every takeover boundary with real auto-restart
    recovery, maintenance window, receipt idempotence, and follow-up batches."""
    release = Path(release).resolve()
    directory = fresh_directory(directory)
    bootstrap = prepare(release, directory)
    rebuilt = directory / "rebuilt.zip"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    log = []
    harness = Harness(directory, env, log)
    first, second, third = batches()
    fixtures = {}
    for batch in (first, second, third):
        path = directory / (batch["batch_id"] + ".json")
        path.write_bytes(pipeline.encode(batch))
        fixtures[batch["batch_id"]] = path
    evidence = {"source_moved": True, "platform": sys.platform}
    started = []
    try:
        # Scenario "main": old primary paused in the ledger hook; takeover must
        # complete without waiting for or killing it, and the resumed stale
        # executor must be fenced off the ledger.
        root, control = directory / "main-service", directory / "main-control"
        install_root(harness, bootstrap, rebuilt, root, control)
        app = root / "app.pyz"
        (control / "a").mkdir(parents=True, exist_ok=True)
        (control / "a" / "hook.json").write_bytes(pipeline.encode({"phase": "ledger"}))
        main = {}
        main["accepted_sample"] = harness.call(root, "submit", root, fixtures["sample"])
        main["agent"] = harness.call(root, "start", root)
        started.append(root)
        main["coordinator_pid"] = wait_for(lambda: service_pid(root), "NO_AGENT_PID")
        ready = wait_for(lambda: (control / "a" / "ledger.ready").exists()
                         and (control / "a" / "ledger.ready").read_text(),
                         "OLD_PRIMARY_NOT_PAUSED")
        main["stale_executor_pid"] = int(ready)
        main["accepted_second"] = harness.call(root, "submit", root, fixtures["second"])
        main["promote_receipt"] = harness.call(root, "promote", root, "REQ-MAIN")
        assert main["promote_receipt"] == {"request_id": "REQ-MAIN",
                                           "frontier": 2, "active": "b"}
        main["failover_status"] = harness.call(root, "failover-status", root)
        assert main["failover_status"]["phase"] == "serving_b"
        assert main["failover_status"]["replicas"] == {"a": 0, "b": 2}
        # New batches are completed by b while the stale executor stays paused.
        main["accepted_third"] = harness.call(root, "submit", root, fixtures["third"])
        assert main["accepted_third"]["seq"] == 3
        wait_for(lambda: harness.call(root, "status", root)["replicas"]["b"] == 3,
                 "STANDBY_NOT_SERVING")
        main["reads_on_b"] = [harness.call(root, "read", root, name)
                              for name in ("sample", "second", "third")]
        assert all(item["available"] for item in main["reads_on_b"])
        main["ledger_before_resume"] = [harness.call(root, "inspect", root, "a", name)
                                        for name in ("sample", "second")]
        assert not any(item["available"] for item in main["ledger_before_resume"])
        # Release the stale executor; it must not commit a late ledger row.
        (control / "a" / "ledger.go").write_text("go")
        wait_for(lambda: (control / "a" / "ledger.spent").exists(), "STALE_NOT_RELEASED")
        wait_for(lambda: (control / "a.stderr").exists()
                 and "FENCED" in (control / "a.stderr").read_text()
                 and (control / "a.stderr").read_text(), "STALE_NOT_FENCED")
        time.sleep(1)
        main["stale_stderr"] = (control / "a.stderr").read_text().strip().splitlines()[-1]
        main["ledger_after_resume"] = [harness.call(root, "inspect", root, "a", name)
                                       for name in ("sample", "second")]
        assert not any(item["available"] for item in main["ledger_after_resume"])
        main["status_after_resume"] = harness.call(root, "status", root)
        assert main["status_after_resume"]["replicas"]["a"] == 0
        main["installed_b"] = [harness.installed(app, root / "b", fixtures[name])
                               for name in ("sample", "second", "third")]
        main["selftest"] = harness.call(root, "selftest")
        main["inprocess_failover"] = harness.checks_failover(app)["regression"]
        evidence["main"] = main
        harness.call(root, "stop", root)
        started.remove(root)

        # Scenario "kill": coordinator SIGKILLed after the frontier persists and
        # around the route switch; the auto-start facility must finish the same
        # request with no manual re-promote.
        root, control = directory / "kill-service", directory / "kill-control"
        install_root(harness, bootstrap, rebuilt, root, control)
        app = root / "app.pyz"
        (control / "hook.json").write_bytes(pipeline.encode(
            {"phase": ["failover-frontier", "failover-preswitch",
                       "failover-postswitch"]}))
        kill = {}
        kill["accepted"] = [harness.call(root, "submit", root, fixtures[name])
                            for name in ("sample", "second")]
        kill["agent"] = harness.call(root, "start", root)
        started.append(root)
        kill["coordinator_pid"] = wait_for(lambda: service_pid(root), "NO_AGENT_PID")
        wait_for(lambda: harness.call(root, "status", root)["replicas"]["a"] == 2,
                 "PRIMARY_NOT_READY")
        promoter = subprocess.Popen(
            [str(sys.executable), str(app), "promote", str(root), "REQ-KILL"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            cwd=directory, env=env)
        kill["promote_cli_pid"] = promoter.pid
        wait_for(lambda: harness.call(root, "failover-status", root)["frontier"] == 2,
                 "FRONTIER_NOT_PERSISTED")
        wait_for(lambda: (control / "failover-frontier.ready").exists(),
                 "FRONTIER_HOOK_NOT_REACHED")
        kill["frontier"] = harness.call(root, "failover-status", root)["frontier"]
        # Kill the coordinator and the promote CLI; recovery must not need either.
        for pid, tag in ((kill["coordinator_pid"], "coordinator"),
                         (promoter.pid, "promote_cli")):
            os.kill(pid, signal.SIGKILL)
            kill.setdefault("sigkill", []).append({"pid": pid, "process": tag})
        promoter.communicate(timeout=30)

        def restarted(after):
            return wait_for(lambda: service_pid(root) not in (None, after)
                            and service_pid(root), "NO_AUTO_RESTART")

        def ready_pid(boundary):
            try:
                return int((control / (boundary + ".ready")).read_text().strip())
            except (OSError, ValueError):
                return None

        boundaries = ("failover-frontier", "failover-preswitch", "failover-postswitch")
        revived = restarted(kill["coordinator_pid"])
        kill.setdefault("restarts", []).append(
            {"boundary": boundaries[0], "old_pid": kill["coordinator_pid"],
             "coordinator_pid": revived, "supervisor_alive": supervisor_alive(control)})
        for index, boundary in enumerate(boundaries):
            # The revived coordinator re-arms the same boundary hook (its .spent
            # was never written), proving recovery resumes the same request.
            wait_for(lambda: ready_pid(boundary) == revived and revived,
                     boundary.upper() + "_NOT_REACHED")
            if boundary == "failover-frontier":
                # Maintenance window: no new log entries, duplicates keep their
                # receipts, reads refuse to splice instances.
                blocked = harness.call(root, "submit", root, fixtures["third"],
                                       expect=75)
                assert blocked["error"] == "MAINTENANCE" and blocked["retryable"]
                kill["maintenance_submit"] = blocked
                kill["duplicate_submit"] = harness.call(root, "submit", root,
                                                        fixtures["sample"])
                assert kill["duplicate_submit"] == kill["accepted"][0]
                kill["maintenance_read"] = harness.call(root, "read", root, "sample",
                                                        expect=75)
                kill["status_mid_takeover"] = harness.call(root, "failover-status", root)
                assert kill["status_mid_takeover"]["phase"] == "takeover"
            (control / (boundary + ".go")).write_text("go")
            if index + 1 == len(boundaries):
                break
            # Let the coordinator pass the released boundary and pause at the
            # next one, then SIGKILL it there.
            following = boundaries[index + 1]
            paused = wait_for(lambda: ready_pid(following),
                              following.upper() + "_NOT_REACHED")
            os.kill(paused, signal.SIGKILL)
            kill["sigkill"].append({"pid": paused, "process": "coordinator",
                                    "boundary": following})
            revived = restarted(paused)
            kill["restarts"].append({"boundary": following, "old_pid": paused,
                                     "coordinator_pid": revived,
                                     "supervisor_alive": supervisor_alive(control)})
        wait_for(lambda: harness.call(root, "failover-status", root)["phase"]
                 == "serving_b", "TAKEOVER_NOT_COMPLETED")
        kill["recovered_status"] = harness.call(root, "failover-status", root)
        assert kill["recovered_status"]["active"] == "b"
        assert kill["recovered_status"]["replicas"] == {"a": 2, "b": 2}
        # Retry of the same request returns the original receipt; a different
        # request id conflicts and never starts a second switch.
        kill["promote_retry"] = harness.call(root, "promote", root, "REQ-KILL")
        assert kill["promote_retry"] == {"request_id": "REQ-KILL",
                                         "frontier": kill["frontier"], "active": "b"}
        kill["promote_conflict"] = harness.call(root, "promote", root, "REQ-OTHER",
                                                expect=2)
        assert kill["promote_conflict"]["error"] == "FAILOVER_CONFLICT"
        kill["accepted_third"] = harness.call(root, "submit", root, fixtures["third"])
        assert kill["accepted_third"]["seq"] == 3
        wait_for(lambda: harness.call(root, "status", root)["replicas"]["b"] == 3,
                 "POST_RECOVERY_BATCH_NOT_DONE")
        kill["read_third"] = harness.call(root, "read", root, "third")
        assert kill["read_third"]["available"]
        kill["installed_b"] = [harness.installed(app, root / "b", fixtures[name])
                               for name in ("sample", "second", "third")]
        evidence["kill"] = kill
        harness.call(root, "stop", root)
        started.remove(root)
        evidence["regression"] = "INSTALLED_TAKEOVER_OK"
        return evidence
    finally:
        for active in started:
            subprocess.run([str(sys.executable), str(Path(active) / "app.pyz"),
                            "stop", str(active)], capture_output=True,
                           cwd=directory, env=env)
        evidence["commands"] = log
        (directory / "evidence.json").write_bytes(pipeline.encode(evidence))


def supervisor_alive(control):
    pidfile = Path(control) / "supervisor.pid"
    if not pidfile.exists():
        return None
    try:
        pid = int(pidfile.read_text())
        os.kill(pid, 0)
    except (ValueError, ProcessLookupError, PermissionError):
        return False
    return True


def demo(release, directory):
    """Concise go-live demo on installed artifacts: real business results on
    the primary, a takeover receipt, and the same receipts served from b."""
    release = Path(release).resolve()
    directory = fresh_directory(directory)
    bootstrap = prepare(release, directory)
    rebuilt = directory / "rebuilt.zip"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    log = []
    harness = Harness(directory, env, log)
    first, second, third = batches()
    fixtures = {}
    for batch in (first, second, third):
        path = directory / (batch["batch_id"] + ".json")
        path.write_bytes(pipeline.encode(batch))
        fixtures[batch["batch_id"]] = path
    evidence = {"source_moved": True, "platform": sys.platform}
    root, control = directory / "service", directory / "control"
    started = False
    try:
        install_root(harness, bootstrap, rebuilt, root, control)
        app = root / "app.pyz"
        evidence["accepted"] = [harness.call(root, "submit", root, fixtures[name])
                                for name in ("sample", "second")]
        evidence["agent"] = harness.call(root, "start", root)
        started = True
        evidence["coordinator_pid"] = wait_for(lambda: service_pid(root), "NO_AGENT_PID")
        wait_for(lambda: harness.call(root, "status", root)["replicas"]["a"] == 2,
                 "PRIMARY_NOT_READY")
        evidence["primary_reads"] = [harness.call(root, "read", root, name)
                                     for name in ("sample", "second")]
        evidence["promote_receipt"] = harness.call(root, "promote", root, "REQ-DEMO")
        evidence["failover_status"] = harness.call(root, "failover-status", root)
        assert evidence["failover_status"]["phase"] == "serving_b"
        evidence["standby_reads"] = [harness.call(root, "read", root, name)
                                     for name in ("sample", "second")]
        assert evidence["standby_reads"] == evidence["primary_reads"]
        evidence["accepted_third"] = harness.call(root, "submit", root,
                                                  fixtures["third"])
        wait_for(lambda: harness.call(root, "status", root)["replicas"]["b"] == 3,
                 "STANDBY_NOT_SERVING")
        evidence["read_third"] = harness.call(root, "read", root, "third")
        evidence["installed_b"] = [harness.installed(app, root / "b", fixtures[name])
                                   for name in ("sample", "second", "third")]
        evidence["regression"] = "INSTALLED_DEMO_OK"
        return evidence
    finally:
        if started:
            harness.call(root, "stop", root)
        evidence["commands"] = log
        (directory / "evidence.json").write_bytes(pipeline.encode(evidence))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("normal", "takeover", "demo"))
    parser.add_argument("release")
    parser.add_argument("directory")
    args = parser.parse_args()
    operation = {"normal": normal, "takeover": takeover, "demo": demo}[args.operation]
    print(json.dumps(operation(args.release, args.directory), sort_keys=True))


if __name__ == "__main__":
    main()
