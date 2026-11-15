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
    elif mode == "installed":
        value = installed(sys.argv[2], json.loads(Path(sys.argv[3]).read_bytes()))
    else:
        raise SystemExit("usage: checks.py normal|verify|demo|installed NODE_ROOT INPUT")
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
