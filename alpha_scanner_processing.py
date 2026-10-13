"""Durable, idempotent SQLite event-processing checkpoints for Alpha Scanner."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc(value: str | None = None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    raw = value[:-1] + "+00:00" if value.endswith("Z") else value
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        raise ValueError("Timestamp must include timezone information")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class DurableProcessing:
    """Append-only events plus atomic output, evidence and checkpoint transactions.

    One consumer checkpoint is maintained per (consumer, stream). The caller must
    submit events in ledger order. A committed retry returns the original output.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._db() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS evidence(
                seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
                body TEXT NOT NULL, previous_hash TEXT NOT NULL, digest TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS events(
                event_id TEXT PRIMARY KEY, stream TEXT NOT NULL, available_at TEXT NOT NULL,
                payload_json TEXT NOT NULL, ledger_seq INTEGER NOT NULL UNIQUE REFERENCES evidence(seq)
            );
            CREATE TABLE IF NOT EXISTS outputs(
                output_id TEXT PRIMARY KEY, consumer TEXT NOT NULL, stream TEXT NOT NULL,
                event_id TEXT NOT NULL REFERENCES events(event_id), event_seq INTEGER NOT NULL,
                output_json TEXT NOT NULL, output_hash TEXT NOT NULL, processed_at TEXT NOT NULL,
                evidence_seq INTEGER NOT NULL UNIQUE REFERENCES evidence(seq),
                UNIQUE(consumer,event_id)
            );
            CREATE TABLE IF NOT EXISTS checkpoints(
                consumer TEXT NOT NULL, stream TEXT NOT NULL, event_seq INTEGER NOT NULL,
                event_id TEXT NOT NULL, output_id TEXT NOT NULL, updated_at TEXT NOT NULL,
                evidence_seq INTEGER NOT NULL REFERENCES evidence(seq),
                PRIMARY KEY(consumer,stream)
            );
            CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
              BEGIN SELECT RAISE(ABORT,'events are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
              BEGIN SELECT RAISE(ABORT,'events are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS outputs_no_update BEFORE UPDATE ON outputs
              BEGIN SELECT RAISE(ABORT,'outputs are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS outputs_no_delete BEFORE DELETE ON outputs
              BEGIN SELECT RAISE(ABORT,'outputs are append-only'); END;
            """)

    def _connect(self):
        c = sqlite3.connect(self.db_path, timeout=15.0)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")
        return c

    class _Transaction:
        def __init__(self, owner):
            self.owner, self.conn = owner, None
        def __enter__(self):
            self.conn = self.owner._connect()
            self.conn.execute("BEGIN IMMEDIATE")
            return self.conn
        def __exit__(self, typ, value, tb):
            try:
                self.conn.rollback() if typ else self.conn.commit()
            finally:
                self.conn.close()
            return False

    def _db(self):
        # Initialization uses executescript; other callers use this transaction wrapper.
        return self._Transaction(self)

    def _append(self, c, kind: str, body: dict[str, Any], at: str) -> tuple[int, str]:
        row = c.execute("SELECT digest FROM evidence ORDER BY seq DESC LIMIT 1").fetchone()
        previous = row["digest"] if row else "GENESIS"
        body_json = _json({"kind": kind, "at": at, "body": body, "previous_hash": previous})
        digest = _hash(body_json)
        cur = c.execute("INSERT INTO evidence(kind,body,previous_hash,digest) VALUES(?,?,?,?)",
                        (kind, body_json, previous, digest))
        return int(cur.lastrowid), digest

    def append_event(self, *, event_id: str, stream: str, available_at: str,
                     payload: dict[str, Any]) -> int:
        if not event_id.strip() or not stream.strip():
            raise ValueError("event_id and stream are required")
        at, payload_json = _utc(available_at), _json(payload)
        with self._db() as c:
            if c.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone():
                raise ValueError(f"duplicate event_id: {event_id}")
            seq, _ = self._append(c, "EVENT", {"event_id":event_id,"stream":stream,
                               "available_at":at,"payload_hash":_hash(payload_json)}, at)
            c.execute("INSERT INTO events VALUES(?,?,?,?,?)",
                      (event_id, stream, at, payload_json, seq))
        return seq

    def _after_output_insert(self) -> None:
        """Fault-injection seam for abrupt-crash tests; production implementation is a no-op."""
        return None

    def commit_output(self, *, consumer: str, stream: str, event_id: str,
                      output: dict[str, Any], processed_at: str | None = None) -> dict[str, Any]:
        if not consumer.strip() or not stream.strip() or not event_id.strip():
            raise ValueError("consumer, stream and event_id are required")
        at, output_json = _utc(processed_at), _json(output)
        output_hash = _hash(output_json)
        with self._db() as c:
            old = c.execute("SELECT * FROM outputs WHERE consumer=? AND event_id=?",
                            (consumer,event_id)).fetchone()
            if old:
                cp = c.execute("SELECT * FROM checkpoints WHERE consumer=? AND stream=?",
                               (consumer,stream)).fetchone()
                if old["stream"] != stream or cp is None or cp["event_seq"] < old["event_seq"]:
                    raise RuntimeError("committed output/checkpoint mismatch")
                return {"output_id":old["output_id"],"event_id":event_id,
                        "output":json.loads(old["output_json"]),"output_hash":old["output_hash"],
                        "already_committed":True,"retry_output_matches":output_hash==old["output_hash"],
                        "checkpoint_event_id":cp["event_id"]}
            event = c.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
            if event is None:
                raise KeyError(event_id)
            if event["stream"] != stream:
                raise ValueError("stream mismatch")
            if event["available_at"] > at:
                raise ValueError("event is not available yet")
            cp = c.execute("SELECT event_seq FROM checkpoints WHERE consumer=? AND stream=?",
                           (consumer,stream)).fetchone()
            previous_seq = int(cp["event_seq"]) if cp else 0
            next_event = c.execute("SELECT event_id,ledger_seq,available_at FROM events "
                                   "WHERE stream=? AND ledger_seq>? ORDER BY ledger_seq LIMIT 1",
                                   (stream,previous_seq)).fetchone()
            if next_event is None or next_event["event_id"] != event_id:
                raise ValueError(f"out-of-order event; expected {next_event['event_id'] if next_event else 'none'}")
            if next_event["available_at"] > at:
                raise ValueError("next event is not available; cannot skip it")
            oid = str(uuid.uuid4())
            body = {"output_id":oid,"consumer":consumer,"stream":stream,"event_id":event_id,
                    "event_seq":int(event["ledger_seq"]),"output_hash":output_hash,
                    "previous_checkpoint_seq":previous_seq}
            out_evidence_seq, out_digest = self._append(c,"PROCESSING_OUTPUT",body,at)
            c.execute("INSERT INTO outputs VALUES(?,?,?,?,?,?,?,?,?)",
                      (oid,consumer,stream,event_id,int(event["ledger_seq"]),output_json,
                       output_hash,at,out_evidence_seq))
            self._after_output_insert()
            cp_body = {"consumer":consumer,"stream":stream,"event_seq":int(event["ledger_seq"]),
                       "event_id":event_id,"output_id":oid,"output_evidence_digest":out_digest}
            cp_seq, _ = self._append(c,"PROCESSING_CHECKPOINT",cp_body,at)
            c.execute("INSERT INTO checkpoints VALUES(?,?,?,?,?,?,?) "
                      "ON CONFLICT(consumer,stream) DO UPDATE SET event_seq=excluded.event_seq,"
                      "event_id=excluded.event_id,output_id=excluded.output_id,updated_at=excluded.updated_at,"
                      "evidence_seq=excluded.evidence_seq",
                      (consumer,stream,int(event["ledger_seq"]),event_id,oid,at,cp_seq))
            return {"output_id":oid,"event_id":event_id,"output":output,
                    "output_hash":output_hash,"already_committed":False,
                    "retry_output_matches":True,"checkpoint_event_id":event_id}

    def checkpoint(self, *, consumer: str, stream: str) -> dict[str, Any] | None:
        with self._connect() as c:
            row = c.execute("SELECT * FROM checkpoints WHERE consumer=? AND stream=?",
                            (consumer,stream)).fetchone()
            return dict(row) if row else None

    def verify(self) -> dict[str, Any]:
        errors, previous = [], "GENESIS"
        with self._connect() as c:
            records = c.execute("SELECT * FROM evidence ORDER BY seq").fetchall()
            for row in records:
                if row["previous_hash"] != previous:
                    errors.append(f"evidence {row['seq']}: previous hash mismatch")
                body = json.loads(row["body"])
                if _hash(row["body"]) != row["digest"]:
                    errors.append(f"evidence {row['seq']}: digest mismatch")
                previous = row["digest"]
            outputs = c.execute("SELECT * FROM outputs ORDER BY consumer,stream,event_seq").fetchall()
            for out in outputs:
                if _hash(out["output_json"]) != out["output_hash"]:
                    errors.append(f"output {out['output_id']}: output hash mismatch")
                cp = c.execute("SELECT * FROM checkpoints WHERE consumer=? AND stream=?",
                               (out["consumer"],out["stream"])).fetchone()
                if cp is None or cp["event_seq"] < out["event_seq"]:
                    errors.append(f"output {out['output_id']}: missing checkpoint")
            cps = c.execute("SELECT * FROM checkpoints").fetchall()
            for cp in cps:
                last = c.execute("SELECT * FROM outputs WHERE consumer=? AND stream=? "
                                  "ORDER BY event_seq DESC LIMIT 1",(cp["consumer"],cp["stream"])).fetchone()
                if last is None or last["event_id"] != cp["event_id"] or last["output_id"] != cp["output_id"]:
                    errors.append(f"checkpoint {cp['consumer']}/{cp['stream']}: does not match latest output")
            events = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return {"ok":not errors,"evidence_checked":len(records),"events_checked":events,
                "outputs_checked":len(outputs),"checkpoints_checked":len(cps),
                "errors":errors,"head_hash":previous}
