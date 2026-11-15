"""Real normal installed primary plus manual standby rehearsal on macOS."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
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
    import re
    target = "gui/%d/%s" % (os.getuid(), ops.identity(root))
    result = subprocess.run(["launchctl", "print", target], capture_output=True, text=True)
    match = re.search(r"\bpid = ([0-9]+)", result.stdout)
    return int(match[1]) if match else None


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("normal",))
    parser.add_argument("release")
    parser.add_argument("directory")
    args = parser.parse_args()
    print(json.dumps(normal(args.release, args.directory), sort_keys=True))


if __name__ == "__main__":
    main()
