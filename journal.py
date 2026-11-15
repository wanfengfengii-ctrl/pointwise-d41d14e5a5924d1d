"""Durable acceptance log, manual replica replay, and one-shot a->b takeover."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

import pipeline
import runtime

LOG_SCHEMA = """
CREATE TABLE inputs(seq INTEGER PRIMARY KEY AUTOINCREMENT,
 id TEXT UNIQUE NOT NULL, digest TEXT NOT NULL, body BLOB NOT NULL);
CREATE TABLE route(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 active TEXT NOT NULL);
INSERT INTO route VALUES(1,'a');
CREATE TABLE failover(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 request_id TEXT NOT NULL, frontier INTEGER NOT NULL,
 phase TEXT NOT NULL, receipt TEXT NOT NULL);
"""

# Takeover phases stored in the failover row: "takeover" (frontier frozen,
# standby not yet switched in) and "done" (serving on b). No row means no
# takeover was ever requested. Every recovery step is idempotent, so a killed
# coordinator simply re-runs the step after the auto-start facility revives it.
class Maintenance(Exception):
    """Raised when a takeover is incomplete; the caller may retry later."""


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


def control_of(root):
    return Path(json.loads((Path(root) / "config.json").read_bytes())["control"])


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
            # Already-accepted batches keep their original acceptance receipt
            # even while a takeover is under way.
            if row[1] != digest:
                raise ValueError("BATCH_CONFLICT")
            seq = row[0]
        else:
            state = db.execute("SELECT phase FROM failover WHERE singleton=1").fetchone()
            if state is not None and state[0] != "done":
                # Takeover incomplete: no new log entries may be accepted.
                raise Maintenance()
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


def replay_locked(root, node):
    """Replay one entry under the replica's own worker lock.

    The per-replica lock serializes catch-up between the promote command and
    the coordinator; it is never a global lock and is only held for one entry,
    so no lock spans a whole catch-up run.
    """
    target = replica(root, node)
    with (target / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return replay_one(root, node)


def failover_state(root):
    with connect(root) as db:
        row = db.execute("SELECT request_id,frontier,phase,receipt FROM failover"
                         " WHERE singleton=1").fetchone()
    if row is None:
        return None
    return {"request_id": row[0], "frontier": row[1], "phase": row[2],
            "receipt": json.loads(row[3])}


def freeze(root, request_id):
    """Persist the takeover request and freeze the accepted-input frontier.

    Runs in one journal write transaction, so it is explicitly ordered against
    every concurrent submit: anything accepted before this transaction is
    inside the frontier, anything after sees the failover row first.
    """
    with connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT request_id,receipt FROM failover"
                         " WHERE singleton=1").fetchone()
        if row is not None:
            if row[0] != request_id:
                raise ValueError("FAILOVER_CONFLICT")
            return json.loads(row[1])  # Retry of the same request: original receipt.
        frontier = db.execute("SELECT COALESCE(MAX(seq),0) FROM inputs").fetchone()[0]
        receipt = {"request_id": request_id, "frontier": frontier, "active": "b"}
        db.execute("INSERT INTO failover VALUES(1,?,?,'takeover',?)",
                   (request_id, frontier, json.dumps(receipt, sort_keys=True)))
        return receipt


def hook(control, phase):
    """Pause a takeover boundary for experiments; never required for recovery.

    Mirrors the existing per-instance hook protocol, but at CONTROL level:
    CONTROL/hook.json holds {"phase": NAME-or-[NAME,...]}; a matching boundary
    writes CONTROL/NAME.ready with its PID, waits for CONTROL/NAME.go, then
    leaves CONTROL/NAME.spent so a restarted coordinator passes exactly once.
    """
    control = Path(control)
    plan = control / "hook.json"
    if not plan.exists():
        return
    phases = json.loads(plan.read_bytes()).get("phase")
    if isinstance(phases, str):
        phases = [phases]
    if not isinstance(phases, list) or phase not in phases:
        return
    spent = control / (phase + ".spent")
    if spent.exists():
        return
    (control / (phase + ".ready")).write_text(str(os.getpid()))
    while not (control / (phase + ".go")).exists():
        time.sleep(0.05)
    spent.write_text("passed")


def failover_step(root):
    """One idempotent recovery pass of the pending takeover; True when done.

    The journal write lock is only ever held for the short freeze/switch/mark
    transactions; replica catch-up (business computation and payload file I/O)
    happens with no global coordination lock held.
    """
    state = failover_state(root)
    if state is None or state["phase"] == "done":
        return True
    frontier = state["frontier"]
    control = control_of(root)
    # Fence the old primary first: after this, a stale executor resumed from a
    # public hook cannot commit ledger on a. Accounting already committed on a
    # before the switch is left untouched.
    runtime.seal(replica(root, "a"))
    hook(control, "failover-frontier")
    # The standby must genuinely account the whole frozen frontier before it
    # may become current; copying the old database or just flipping the route
    # is not sufficient.
    while cursor(root, "b") < frontier:
        if not replay_locked(root, "b"):
            raise ValueError("FRONTIER_MISSING")
    hook(control, "failover-preswitch")
    with connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE route SET active='b' WHERE singleton=1")
    hook(control, "failover-postswitch")
    with connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE failover SET phase='done' WHERE singleton=1")
    return True


def promote(root, request_id):
    if not request_id or any(c not in
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            for c in request_id):
        raise ValueError("REQUEST_ID")
    receipt = freeze(root, request_id)
    while not failover_step(root):
        pass
    return receipt


def read(root, name):
    state = failover_state(root)
    if state is not None and state["phase"] != "done":
        # Exactly one instance ever serves; during a takeover that is nobody.
        raise Maintenance()
    return runtime.read(replica(root, current(root)), name)


def phase_of(state):
    if state is None:
        return "serving"
    return "takeover" if state["phase"] != "done" else "serving_b"


def status(root):
    return _status(root, failover_state(root))


def _status(root, state):
    with connect(root) as db:
        active = db.execute("SELECT active FROM route WHERE singleton=1").fetchone()[0]
        accepted = db.execute("SELECT COALESCE(MAX(seq),0) FROM inputs").fetchone()[0]
    return {"active": active, "accepted_seq": accepted,
            "replicas": {node: cursor(root, node) for node in ("a", "b")},
            "phase": phase_of(state)}


def failover_status(root):
    # The failover row is read once so phase, request_id and frontier always
    # come from the same snapshot.
    state = failover_state(root)
    base = _status(root, state)
    return {"request_id": state["request_id"] if state else None,
            "frontier": state["frontier"] if state else None,
            "active": base["active"], "accepted_seq": base["accepted_seq"],
            "replicas": base["replicas"], "phase": base["phase"]}
