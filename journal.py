"""Durable acceptance log and manual replica replay; no failover policy."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3

import pipeline
import runtime

LOG_SCHEMA = """
CREATE TABLE inputs(seq INTEGER PRIMARY KEY AUTOINCREMENT,
 id TEXT UNIQUE NOT NULL, digest TEXT NOT NULL, body BLOB NOT NULL);
CREATE TABLE route(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 active TEXT NOT NULL);
INSERT INTO route VALUES(1,'a');
"""


@contextmanager
def connect(root):
    db = sqlite3.connect(str(Path(root) / "journal.sqlite"), timeout=5)
    try:
        db.execute("PRAGMA synchronous=FULL")
        with db:
            yield db
    finally:
        db.close()


def replica(root, node):
    if node not in ("a", "b"):
        raise ValueError("NODE")
    return Path(root) / node


def init(root, control):
    root, control = Path(root).resolve(), Path(control).resolve()
    root.mkdir(parents=True, exist_ok=True)
    control.mkdir(parents=True, exist_ok=True)
    with connect(root) as db:
        db.executescript(LOG_SCHEMA)
    for node in ("a", "b"):
        runtime.init(replica(root, node), control / node)
        with runtime.connect(replica(root, node)) as db:
            db.execute("CREATE TABLE replay_cursor(singleton INTEGER PRIMARY KEY, seq INTEGER)")
            db.execute("INSERT INTO replay_cursor VALUES(1,0)")
    (root / "config.json").write_bytes(pipeline.encode({"control": str(control)}))


def accept(db, batch):
    pipeline.validate(batch)
    body = pipeline.encode(batch)
    digest = hashlib.sha256(body).hexdigest()
    with db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT seq,digest FROM inputs WHERE id=?",
                         (batch["batch_id"],)).fetchone()
        if row is not None:
            if row[1] != digest:
                raise ValueError("BATCH_CONFLICT")
            seq = row[0]
        else:
            seq = db.execute("INSERT INTO inputs(id,digest,body) VALUES(?,?,?)",
                             (batch["batch_id"], digest, body)).lastrowid
    return {"accepted": batch["batch_id"], "seq": seq, "input_sha256": digest}


def submit(root, batch):
    with connect(root) as db:
        return accept(db, batch)


def current(root):
    with connect(root) as db:
        return db.execute("SELECT active FROM route WHERE singleton=1").fetchone()[0]


def cursor(root, node):
    with runtime.connect(replica(root, node)) as db:
        return db.execute("SELECT seq FROM replay_cursor WHERE singleton=1").fetchone()[0]


def replay_one(root, node):
    target = replica(root, node)
    after = cursor(root, node)
    with connect(root) as db:
        entry = db.execute("SELECT seq,body FROM inputs WHERE seq>? ORDER BY seq LIMIT 1",
                           (after,)).fetchone()
    if entry is None:
        return False
    seq, body = entry
    batch = json.loads(body)
    runtime.submit(target, batch)
    if not runtime.read(target, batch["batch_id"])["available"]:
        runtime.run_one(target)
    actual = runtime.read(target, batch["batch_id"])
    if not actual["available"]:
        raise ValueError("REPLAY_INCOMPLETE")
    with runtime.connect(target) as db:
        db.execute("UPDATE replay_cursor SET seq=? WHERE singleton=1", (seq,))
    return True


def read(root, name):
    return runtime.read(replica(root, current(root)), name)


def status(root):
    with connect(root) as db:
        active = db.execute("SELECT active FROM route WHERE singleton=1").fetchone()[0]
        accepted = db.execute("SELECT COALESCE(MAX(seq),0) FROM inputs").fetchone()[0]
    return {"active": active, "accepted_seq": accepted,
            "replicas": {node: cursor(root, node) for node in ("a", "b")},
            "phase": "serving"}
