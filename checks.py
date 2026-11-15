"""Frozen business oracle and helpers for installed regression."""
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys

import pipeline
import runtime


def small():
    rows = [
        ("a", 1, "east", 0, 3, False),
        ("b", 1, "east", 0, -3, False),
        ("a", 2, "west", 1, 7, False),
        ("c", 1, "east", 0, 3, False),
        ("d", 1, "east", 2, 9, False),
        ("d", 2, "east", 2, 9, True),
    ]
    return {"batch_id": "sample", "scale": 10,
            "rows": [dict(zip(("id", "rev", "station", "minute", "value", "deleted"), r))
                     for r in rows]}


def expected(batch):
    # Independent oracle; never uses the stage or summarizer.
    winners = {}
    for row in batch["rows"]:
        old = winners.get(row["id"])
        if old is None or old["rev"] < row["rev"]:
            winners[row["id"]] = row
    cells = {}
    for row in winners.values():
        if row["deleted"]:
            continue
        key = (row["station"], row["minute"])
        count, total = cells.get(key, (0, 0))
        cells[key] = (count + 1, total + batch["scale"] * row["value"])
    return [{"station": s, "minute": m, "count": n, "sum": total}
            for (s, m), (n, total) in sorted(cells.items())]


def normal():
    batch = small()
    stream = io.BytesIO()
    pipeline.stage(batch, stream)
    stream.seek(0)
    result = pipeline.summarize(stream)
    assert result == [{"station": "east", "minute": 0, "count": 2, "sum": 0},
                      {"station": "west", "minute": 1, "count": 1, "sum": 70}]
    assert result == expected(batch)
    body = pipeline.encode(result)
    digest = hashlib.sha256(pipeline.encode(batch)).hexdigest()
    ack = pipeline.receipt("sample", digest, body)
    db = sqlite3.connect(":memory:")
    db.executescript(runtime.SCHEMA)
    db.execute("INSERT INTO jobs(id,digest,state) VALUES(?,?,'running')", ("sample", digest))
    db.commit()
    runtime.commit_result(db, "sample", body, ack, "results/sample.json")
    assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (1,)
    assert db.execute("SELECT state FROM jobs").fetchone() == ("completed",)
    assert json.loads(db.execute("SELECT result FROM ledger").fetchone()[0]) == result
    assert json.loads(db.execute("SELECT receipt FROM ledger").fetchone()[0]) == ack
    db.close()
    bad = small()
    bad["rows"].append(dict(bad["rows"][0], value=99))
    try:
        pipeline.validate(bad)
    except ValueError as error:
        assert str(error) == "REVISION_CONFLICT"
    else:
        raise AssertionError("conflicting revision accepted")
    return {"regression": "NORMAL_REGRESSION_OK", "result": result, "receipt": ack}


def installed(root, batch):
    actual = runtime.read(root, batch["batch_id"])
    assert actual["available"] and actual["result"] == expected(batch), "RESULT_MISMATCH"
    saved = (Path(root) / "inputs" / (batch["batch_id"] + ".json")).read_bytes()
    assert saved == pipeline.encode(batch), "ACCEPTED_INPUT_CHANGED"
    ack = actual["receipt"]
    assert ack == pipeline.receipt(batch["batch_id"], hashlib.sha256(saved).hexdigest(),
                                   pipeline.encode(expected(batch))), "RECEIPT_MISMATCH"
    accepted = runtime.submit(root, batch)
    assert accepted["input_sha256"] == ack["input_sha256"]
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM ledger WHERE id=?",
                          (batch["batch_id"],)).fetchone() == (1,), "DUPLICATE_ACCOUNTING"
        assert json.loads(db.execute("SELECT result FROM ledger WHERE id=?",
                                     (batch["batch_id"],)).fetchone()[0]) == expected(batch)
    assert runtime.read(root, batch["batch_id"]) == actual
    return {"installed": "INSTALLED_REGRESSION_OK", "receipt": ack,
            "groups": len(actual["result"])}


def second_batch():
    return {"batch_id": "second", "scale": 2, "rows": [
        {"id": "x", "rev": 1, "station": "north", "minute": 2,
         "value": 8, "deleted": False},
        {"id": "x", "rev": 2, "station": "south", "minute": 3,
         "value": -4, "deleted": False},
        {"id": "y", "rev": 1, "station": "south", "minute": 3,
         "value": 4, "deleted": False}]}


def third_batch():
    return {"batch_id": "third", "scale": 1, "rows": [
        {"id": "z", "rev": 1, "station": "north", "minute": 5,
         "value": 11, "deleted": False}]}


def failover():
    """In-process takeover regression: maintenance window, idempotent receipt,
    conflicting request, standby catch-up, and stale-executor fencing."""
    import tempfile
    import threading
    import time
    import journal

    def wait_file(path, message, seconds=30):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if Path(path).exists():
                return Path(path).read_text()
            time.sleep(0.05)
        raise AssertionError(message)

    report = {}
    with tempfile.TemporaryDirectory(prefix="warehouse failover ") as directory:
        directory = Path(directory)
        root, control = directory / "installed", directory / "control"
        journal.init(root, control)
        first, second, third = small(), second_batch(), third_batch()
        assert journal.submit(root, first)["seq"] == 1
        assert journal.submit(root, second)["seq"] == 2
        assert journal.replay_one(root, "a") and journal.replay_one(root, "a")
        # Hold the takeover at the preswitch boundary to observe maintenance.
        (control / "hook.json").write_bytes(
            pipeline.encode({"phase": ["failover-preswitch"]}))
        outcome = {}
        promoter = threading.Thread(
            target=lambda: outcome.setdefault("receipt", journal.promote(root, "REQ-1")))
        promoter.start()
        wait_file(control / "failover-preswitch.ready", "PRESWITCH_NOT_REACHED")
        paused = journal.failover_status(root)
        assert paused["phase"] == "takeover" and paused["active"] == "a"
        assert paused["request_id"] == "REQ-1" and paused["frontier"] == 2
        try:
            journal.submit(root, third)
        except journal.Maintenance:
            report["maintenance_submit"] = True
        else:
            raise AssertionError("new batch accepted during takeover")
        assert journal.submit(root, first) == {"accepted": "sample", "seq": 1,
            "input_sha256": hashlib.sha256(pipeline.encode(first)).hexdigest()}
        try:
            journal.read(root, "sample")
        except journal.Maintenance:
            report["maintenance_read"] = True
        else:
            raise AssertionError("read served during takeover")
        (control / "failover-preswitch.go").write_text("go")
        promoter.join(30)
        receipt = {"request_id": "REQ-1", "frontier": 2, "active": "b"}
        assert outcome["receipt"] == receipt
        assert journal.promote(root, "REQ-1") == receipt, "retry changed the receipt"
        try:
            journal.promote(root, "REQ-2")
        except ValueError as error:
            assert str(error) == "FAILOVER_CONFLICT"
        else:
            raise AssertionError("second takeover request accepted")
        done = journal.failover_status(root)
        assert done == {"request_id": "REQ-1", "frontier": 2, "active": "b",
                        "accepted_seq": 2, "replicas": {"a": 2, "b": 2},
                        "phase": "serving_b"}
        installed(journal.replica(root, "b"), first)
        installed(journal.replica(root, "b"), second)
        # A new batch after the switch is accepted and accounted by b.
        assert journal.submit(root, third)["seq"] == 3
        assert journal.replay_one(root, "b")
        installed(journal.replica(root, "b"), third)
        report["takeover"] = {"receipt": receipt, "status": done}

        # Fencing: a stale executor paused in the stage hook must not commit
        # ledger after the takeover seals replica a.
        root2, control2 = directory / "fenced", directory / "control2"
        journal.init(root2, control2)
        journal.submit(root2, first)
        (control2 / "a").mkdir(parents=True, exist_ok=True)
        (control2 / "a" / "hook.json").write_bytes(pipeline.encode({"phase": "stage"}))
        failure = {}

        def stale_executor():
            try:
                journal.replay_one(root2, "a")
            except ValueError as error:
                failure["error"] = str(error)

        executor = threading.Thread(target=stale_executor)
        executor.start()
        wait_file(control2 / "a" / "stage.ready", "STAGE_HOOK_NOT_REACHED")
        assert journal.promote(root2, "REQ-9") == {"request_id": "REQ-9",
                                                   "frontier": 1, "active": "b"}
        (control2 / "a" / "stage.go").write_text("go")
        executor.join(30)
        assert failure.get("error") == "FENCED", failure
        assert not runtime.read(journal.replica(root2, "a"), "sample")["available"]
        with runtime.connect(journal.replica(root2, "a")) as db:
            assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (0,)
            assert db.execute("SELECT seq FROM replay_cursor").fetchone() == (0,)
        installed(journal.replica(root2, "b"), first)
        report["fencing"] = {"stale_executor": "FENCED", "a_ledger_rows": 0}

        # Takeover with no inputs at all freezes an empty frontier.
        root3 = directory / "empty"
        journal.init(root3, directory / "control3")
        assert journal.promote(root3, "REQ-0") == {"request_id": "REQ-0",
                                                   "frontier": 0, "active": "b"}
        assert journal.failover_status(root3)["phase"] == "serving_b"
        report["empty_frontier"] = True
    report["regression"] = "FAILOVER_REGRESSION_OK"
    return report


def verify():
    import tempfile
    import journal
    report = {"business": normal()}
    with tempfile.TemporaryDirectory(prefix="warehouse baseline ") as directory:
        root = Path(directory) / "installed"
        journal.init(root, Path(directory) / "control")
        first = small()
        second = {"batch_id": "second", "scale": 2, "rows": [
            {"id": "x", "rev": 1, "station": "north", "minute": 2,
             "value": 8, "deleted": False},
            {"id": "x", "rev": 2, "station": "south", "minute": 3,
             "value": -4, "deleted": False},
            {"id": "y", "rev": 1, "station": "south", "minute": 3,
             "value": 4, "deleted": False}]}
        accepted = journal.submit(root, first)
        assert accepted["seq"] == 1
        assert journal.submit(root, first) == accepted
        first["rows"][0]["value"] = 50
        try:
            journal.submit(root, first)
        except ValueError as error:
            assert str(error) == "BATCH_CONFLICT"
        else:
            raise AssertionError("changed input accepted")
        assert journal.submit(root, second)["seq"] == 2
        assert not journal.read(root, "sample")["available"]
        for node in ("a", "b"):
            assert journal.replay_one(root, node)
            # Simulate local ledger committed but replay cursor not yet advanced.
            with runtime.connect(journal.replica(root, node)) as db:
                db.execute("UPDATE replay_cursor SET seq=0")
            assert journal.replay_one(root, node)
            assert journal.replay_one(root, node)
            assert not journal.replay_one(root, node)
            for batch in (small(), second):
                installed(journal.replica(root, node), batch)
        assert journal.read(root, "second")["result"] == [
            {"station": "south", "minute": 3, "count": 2, "sum": 0}]
        assert journal.status(root) == {"active": "a", "accepted_seq": 2,
             "replicas": {"a": 2, "b": 2}, "phase": "serving"}
        report["replicas"] = {"regression": "MANUAL_REPLAY_OK",
             "status": journal.status(root), "sample": journal.read(root, "sample"),
             "second": journal.read(root, "second")}
    return report


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if mode == "normal":
        value = normal()
    elif mode in ("verify", "demo"):
        value = verify()
    elif mode == "failover":
        value = failover()
    elif mode == "installed":
        value = installed(sys.argv[2], json.loads(Path(sys.argv[3]).read_bytes()))
    else:
        raise SystemExit("usage: checks.py normal|verify|demo|failover|installed NODE_ROOT INPUT")
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
