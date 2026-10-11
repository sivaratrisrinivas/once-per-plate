"""The ledger: every state change for a run goes through here, in Postgres transactions.

States: RECEIVED -> QUEUED -> RUNNING -> PROPOSED -> APPROVED -> WRITTEN
        PROPOSED -> REJECTED; RUNNING -> PARKED (malformed); any open state -> SUPERSEDED
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

import psycopg

from . import elispot
from .db import connect

OPEN = ("RECEIVED", "QUEUED", "RUNNING", "PROPOSED")
TERMINAL = ("WRITTEN", "REJECTED", "SUPERSEDED", "PARKED")


def _event(cur, run_id, actor, kind, detail="", to_state=None):
    cur.execute("INSERT INTO events (run_id, actor, kind, to_state, detail) VALUES (%s,%s,%s,%s,%s)",
                (run_id, actor, kind, to_state, detail))


def _set_state(cur, run_id, state, actor, kind, detail="", **cols):
    sets = ", ".join([f"{k}=%s" for k in cols] + ["state=%s", "updated_at=now()"])
    cur.execute(f"UPDATE runs SET {sets} WHERE id=%s", (*cols.values(), state, run_id))
    _event(cur, run_id, actor, kind, detail, to_state=state)


def ingest(content: bytes, filename: str, source: str = "folder") -> dict:
    """Record one arrival. Same bytes again links to the existing run; changed bytes for the same
    plate make a new version. Returns {run_id, outcome, publish}. The caller publishes after commit."""
    sha = hashlib.sha256(content).hexdigest()
    meta = elispot.read_meta(content)
    plate = meta.plate_id if meta else f"UNREADABLE-{sha[:10]}"
    instrument = meta.instrument_id if meta else "unknown"
    exported = meta.exported_at if meta else None
    run_key = f"{sha}:{instrument}:{plate}"
    with connect() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO blobs (sha256, content, size) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                    (sha, content, len(content)))
        # Serialize arrivals for one plate so versions are assigned in order.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (plate,))
        cur.execute("SELECT id, state, version FROM runs WHERE run_key=%s", (run_key,))
        existing = cur.fetchone()
        if existing:
            cur.execute("INSERT INTO arrivals (filename, source, sha256, size, outcome, run_id) "
                        "VALUES (%s,%s,%s,%s,'DUPLICATE',%s)", (filename, source, sha, len(content), existing["id"]))
            _event(cur, existing["id"], source, "duplicate_arrival",
                   f"{filename}: same bytes as v{existing['version']}, linked (run is {existing['state']})")
            return {"run_id": existing["id"], "outcome": "DUPLICATE", "publish": False, "sha256": sha}
        cur.execute("SELECT id, state, version, exported_at FROM runs WHERE plate_id=%s "
                    "ORDER BY version DESC LIMIT 1", (plate,))
        latest = cur.fetchone()
        version = latest["version"] + 1 if latest else 1
        stale = bool(latest and exported and latest["exported_at"] and exported < latest["exported_at"])
        outcome = "STALE" if stale else ("NEW_VERSION" if latest else "NEW_RUN")
        state = "SUPERSEDED" if stale else "RECEIVED"
        cur.execute("INSERT INTO runs (run_key, sha256, plate_id, instrument_id, exported_at, version, "
                    "supersedes_run_id, state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                    (run_key, sha, plate, instrument, exported, version,
                     None if stale or not latest else latest["id"], state))
        run_id = cur.fetchone()["id"]
        cur.execute("INSERT INTO arrivals (filename, source, sha256, size, outcome, run_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s)", (filename, source, sha, len(content), outcome, run_id))
        if stale:
            _event(cur, run_id, source, "stale_arrival",
                   f"{filename}: exported before v{latest['version']}, kept for audit, not run", "SUPERSEDED")
        else:
            detail = f"{filename} sha256 {sha[:12]}"
            if latest:
                detail += f", supersedes v{latest['version']} (run {latest['id']}, {latest['state']})"
            _event(cur, run_id, source, "received", detail, "RECEIVED")
            if latest and latest["state"] in OPEN:
                _set_state(cur, latest["id"], "SUPERSEDED", source, "superseded",
                           f"v{version} arrived (run {run_id}); this version needs no approval",
                           lease_owner=None)
        return {"run_id": run_id, "outcome": outcome, "publish": not stale, "sha256": sha}


def mark_published(run_id: int) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE runs SET published_at=now() WHERE id=%s", (run_id,))
        cur.execute("UPDATE runs SET state='QUEUED', updated_at=now() WHERE id=%s AND state='RECEIVED' "
                    "RETURNING id", (run_id,))
        if cur.fetchone():
            _event(cur, run_id, "publisher", "queued", "message confirmed by RabbitMQ", "QUEUED")


def claim(run_id: int, worker: str, lease_seconds: float) -> str:
    """Take the run for this worker. Returns claimed, busy (another live worker holds it), done, missing."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, lease_owner, lease_until > now() AS live, attempts FROM runs "
                    "WHERE id=%s FOR UPDATE SKIP LOCKED", (run_id,))
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT 1 FROM runs WHERE id=%s", (run_id,))
            return "busy" if cur.fetchone() else "missing"
        st = row["state"]
        if st in ("RECEIVED", "QUEUED") or (st == "RUNNING" and not row["live"]):
            if st == "RUNNING":
                kind, detail = "resumed", f"lease of {row['lease_owner']} expired; {worker} takes over"
            else:
                kind, detail = "claimed", f"{worker} starts QC"
            _set_state(cur, run_id, "RUNNING", worker, kind, detail + f" (attempt {row['attempts'] + 1})",
                       lease_owner=worker, lease_until=_utc_in(lease_seconds), attempts=row["attempts"] + 1)
            return "claimed"
        if st == "RUNNING":
            return "claimed" if row["lease_owner"] == worker else "busy"
        return "done"


def _utc_in(seconds):
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def renew(run_id: int, worker: str, lease_seconds: float) -> bool:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE runs SET lease_until=%s WHERE id=%s AND state='RUNNING' AND lease_owner=%s",
                    (_utc_in(lease_seconds), run_id, worker))
        return cur.rowcount == 1


def note_duplicate_delivery(run_id: int, worker: str, state_hint: str) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
        if row:
            _event(cur, run_id, worker, "duplicate_delivery",
                   f"message delivered again; run already {row['state']}, ignored ({state_hint})")


def load_content(run_id: int) -> bytes:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT b.content FROM runs r JOIN blobs b USING (sha256) WHERE r.id=%s", (run_id,))
        return bytes(cur.fetchone()["content"])


def finalize(run_id: int, worker: str, result: dict) -> bool:
    """Store the proposal only if this worker still holds the run."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, lease_owner FROM runs WHERE id=%s FOR UPDATE", (run_id,))
        row = cur.fetchone()
        if not row or row["state"] != "RUNNING" or row["lease_owner"] != worker:
            _event(cur, run_id, worker, "late_finish_discarded",
                   f"finished but run is {row['state'] if row else '?'} / held by {row and row['lease_owner']}")
            return False
        tally = result["tally"]
        _set_state(cur, run_id, "PROPOSED", worker, "proposed",
                   "QC done, waiting for approval: " + ", ".join(f"{v} {k}" for k, v in sorted(tally.items())),
                   result=json.dumps(result), summary=json.dumps(tally), lease_owner=None, lease_until=None)
        return True


def park(run_id: int, worker: str, reason: str) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state FROM runs WHERE id=%s FOR UPDATE", (run_id,))
        row = cur.fetchone()
        if row and row["state"] in OPEN:
            _set_state(cur, run_id, "PARKED", worker, "parked", f"malformed export: {reason}",
                       lease_owner=None, lease_until=None, note=reason)


def approve(run_id: int, approver: str) -> dict:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, decided_by FROM runs WHERE id=%s FOR UPDATE", (run_id,))
        row = cur.fetchone()
        if not row:
            return {"ok": False, "status": 404, "detail": "no such run"}
        if row["state"] in ("APPROVED", "WRITTEN"):
            _event(cur, run_id, approver, "approve_repeat", f"already approved by {row['decided_by']}; no-op")
            return {"ok": True, "status": 200, "detail": f"already approved by {row['decided_by']}", "repeat": True}
        if row["state"] != "PROPOSED":
            return {"ok": False, "status": 409, "detail": f"run is {row['state']}"}
        _set_state(cur, run_id, "APPROVED", approver, "approved", f"approved by {approver}",
                   decided_by=approver, decided_at=_utc_in(0))
    return {"ok": True, "status": 200, "detail": "approved", "repeat": False}


def reject(run_id: int, approver: str, reason: str = "") -> dict:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, decided_by FROM runs WHERE id=%s FOR UPDATE", (run_id,))
        row = cur.fetchone()
        if not row:
            return {"ok": False, "status": 404, "detail": "no such run"}
        if row["state"] == "REJECTED":
            return {"ok": True, "status": 200, "detail": f"already rejected by {row['decided_by']}"}
        if row["state"] != "PROPOSED":
            return {"ok": False, "status": 409, "detail": f"run is {row['state']}"}
        _set_state(cur, run_id, "REJECTED", approver, "rejected", f"rejected by {approver}: {reason}",
                   decided_by=approver, decided_at=_utc_in(0), note=reason)
    return {"ok": True, "status": 200, "detail": "rejected"}


class WriteAckLost(Exception):
    """Test hook: the rows committed but the caller never heard back (a timeout)."""


def write_back(run_id: int, actor: str = "writer", lose_ack: bool = False) -> dict:
    """Write the approved run's rows to the labsheet table. Safe to call any number of times."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, plate_id, result FROM runs WHERE id=%s FOR UPDATE", (run_id,))
        row = cur.fetchone()
        if not row or row["state"] not in ("APPROVED", "WRITTEN"):
            return {"written": 0, "state": row and row["state"]}
        if row["state"] == "WRITTEN":
            _event(cur, run_id, actor, "write_repeat", "rows already written; no-op")
            return {"written": 0, "state": "WRITTEN"}
        new = 0
        for w in row["result"]["wells"]:
            cur.execute("INSERT INTO labsheet_rows (run_id, well, plate_id, sample_id, condition, spot_count, "
                        "verdict) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id, well) DO NOTHING",
                        (run_id, w["well"], row["plate_id"], w["sample_id"], w["condition"], w["spot_count"],
                         w["verdict"]))
            new += cur.rowcount
        _set_state(cur, run_id, "WRITTEN", actor, "written", f"{new} labsheet rows written")
    if lose_ack:
        raise WriteAckLost(run_id)
    return {"written": new, "state": "WRITTEN"}


def rebuild_states() -> dict[int, str]:
    """Replay the append-only event log to rebuild every run's state."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT run_id, to_state FROM events WHERE to_state IS NOT NULL ORDER BY id")
        state = {}
        for e in cur.fetchall():
            state[e["run_id"]] = e["to_state"]
        return state


def run_states() -> dict[int, str]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, state FROM runs")
        return {r["id"]: r["state"] for r in cur.fetchall()}
