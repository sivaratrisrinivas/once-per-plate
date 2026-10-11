"""HTTP surface: an arrival endpoint, the approval gate, and a one-page view of the ledger."""
from __future__ import annotations

import json
import os
import random
import signal
import time

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from . import broker, config, db, elispot, ledger
from .db import connect
from .watcher import ingest_and_publish

app = FastAPI(title="Once Per Plate")
pub = broker.Publisher()
HERE = os.path.dirname(__file__)
ROOT = os.path.dirname(HERE)
DEMO = os.environ.get("OPP_DEMO", "1") == "1"


@app.on_event("startup")
def _startup():
    db.init_schema()
    os.makedirs(config.DROP_DIR, exist_ok=True)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


@app.get("/demo.mp4")
def video():
    p = os.path.join(ROOT, "docs", "demo.mp4")
    if not os.path.exists(p):
        raise HTTPException(404)
    return FileResponse(p, media_type="video/mp4")


@app.get("/api/chaos")
def chaos():
    p = os.path.join(ROOT, "docs", "chaos-results.json")
    return JSONResponse(json.load(open(p)) if os.path.exists(p) else {"cases": []})


@app.post("/api/arrivals")
async def arrival(request: Request, x_filename: str = Header("upload.csv"),
                  event_type: str | None = Header(None)):
    """Webhook-style arrival: raw export bytes in the body. Same bytes twice link to one run."""
    content = await request.body()
    r = ingest_and_publish(content, x_filename, f"http:{event_type or 'file.arrived'}", pub)
    return JSONResponse(r, status_code=201 if r["outcome"] in ("NEW_RUN", "NEW_VERSION") else 200)


class Decision(BaseModel):
    approver: str = "analyst (demo, no login)"
    reason: str = ""


@app.post("/api/runs/{run_id}/approve")
def approve(run_id: int, d: Decision):
    r = ledger.approve(run_id, d.approver)
    if not r["ok"]:
        raise HTTPException(r["status"], r["detail"])
    w = ledger.write_back(run_id, actor=d.approver)
    return {**r, **w}


@app.post("/api/runs/{run_id}/reject")
def reject(run_id: int, d: Decision):
    r = ledger.reject(run_id, d.approver, d.reason or "rejected in review")
    if not r["ok"]:
        raise HTTPException(r["status"], r["detail"])
    return r


def _audit_line(run, arrivals, events):
    kinds = [e["kind"] for e in events]
    parts = [f"{arrivals} arrival{'s' if arrivals != 1 else ''}, 1 run"]
    dups = kinds.count("duplicate_delivery")
    if dups:
        parts.append(f"{dups} duplicate message{'s' if dups > 1 else ''} ignored")
    if "resumed" in kinds:
        parts.append(f"worker died, resumed on attempt {run['attempts']}")
    if kinds.count("approve_repeat"):
        parts.append(f"approve clicked {kinds.count('approve_repeat') + 1}x, applied once")
    return "; ".join(parts)


@app.get("/api/state")
def state():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT r.id, r.plate_id, r.version, r.state, r.attempts, r.lease_owner, r.summary,
                              r.decided_by, r.note, r.sha256, r.supersedes_run_id,
                              (SELECT count(*) FROM arrivals a WHERE a.run_id=r.id) AS arrivals,
                              (SELECT count(*) FROM labsheet_rows l WHERE l.run_id=r.id) AS rows
                       FROM runs r ORDER BY r.id DESC LIMIT 120""")
        runs = cur.fetchall()
        cur.execute("SELECT run_id, kind, actor, detail, at FROM events WHERE run_id = ANY(%s) ORDER BY id",
                    ([r["id"] for r in runs],))
        ev = {}
        for e in cur.fetchall():
            ev.setdefault(e["run_id"], []).append(e)
        cur.execute("""SELECT (SELECT count(*) FROM arrivals) AS files,
                              (SELECT count(*) FROM arrivals WHERE outcome='DUPLICATE') AS duplicates,
                              (SELECT count(*) FROM runs) AS runs,
                              (SELECT count(DISTINCT plate_id) FROM runs) AS plates,
                              (SELECT count(*) FROM runs WHERE state='PROPOSED') AS awaiting,
                              (SELECT count(*) FROM runs WHERE state IN ('RECEIVED','QUEUED','RUNNING')) AS in_flight,
                              (SELECT count(*) FROM runs WHERE state='WRITTEN') AS written,
                              (SELECT count(*) FROM runs WHERE state='PARKED') AS parked,
                              (SELECT count(*) FROM labsheet_rows) AS rows,
                              (SELECT count(*) FROM events WHERE kind='resumed') AS resumed,
                              (SELECT count(*) FROM events WHERE kind='duplicate_delivery') AS dup_msgs""")
        counts = cur.fetchone()
    pending = len([n for n in os.listdir(config.DROP_DIR) if os.path.isfile(os.path.join(config.DROP_DIR, n))
                   and not n.endswith(".sha256")]) if os.path.isdir(config.DROP_DIR) else 0
    out = []
    for r in runs:
        events = ev.get(r["id"], [])
        out.append({**{k: r[k] for k in r if k not in ("sha256",)}, "sha": r["sha256"][:10],
                    "audit": _audit_line(r, r["arrivals"], events),
                    "events": [{"kind": e["kind"], "actor": e["actor"], "detail": e["detail"],
                                "at": e["at"].isoformat()} for e in events]})
    return {"counts": {**counts, "pending_files": pending}, "queue": pub.counts(), "runs": out}


@app.get("/api/runs/{run_id}")
def run_detail(run_id: int):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, plate_id, version, state, result, decided_by FROM runs WHERE id=%s", (run_id,))
        r = cur.fetchone()
        if not r:
            raise HTTPException(404)
        return r


# ---- demo controls (synthetic data only) ----

def _drop(name: str, content: bytes):
    tmp = os.path.join(config.DROP_DIR, name + ".part")
    with open(tmp, "wb") as f:
        f.write(content)
    os.replace(tmp, os.path.join(config.DROP_DIR, name))


class DropReq(BaseModel):
    plate: int = 1
    copies: int = 1
    tweak: bool = False


@app.post("/api/demo/reset")
def demo_reset():
    if not DEMO:
        raise HTTPException(404)
    db.reset()
    try:
        pub.purge()
    except Exception:
        pass
    return {"ok": True}


@app.post("/api/demo/drop")
def demo_drop(d: DropReq):
    plate = f"PLT-{d.plate:04d}"
    content = elispot.make_export(plate, d.plate, tweak={"B5": 41} if d.tweak else None,
                                  exported_at=None)
    if d.tweak:  # a re-export later the same night
        content = content.replace(b"ExportedAt: 2026-10-12T", b"ExportedAt: 2026-10-13T", 1)
    for i in range(d.copies):
        suffix = "-reexport" if d.tweak else ("" if i == 0 else f" (copy {i})")
        _drop(f"{plate}{suffix}.csv", content)
        time.sleep(0.05)
    return {"plate": plate}


@app.post("/api/demo/burst")
def demo_burst(n: int = 50, dups: int = 10):
    base = random.randint(100, 8000)
    plates = [base + i for i in range(n)]
    for p in plates:
        _drop(f"PLT-{p:04d}.csv", elispot.make_export(f"PLT-{p:04d}", p))
    for p in random.sample(plates, dups):
        _drop(f"PLT-{p:04d}-resync.csv", elispot.make_export(f"PLT-{p:04d}", p))
    return {"files": n + dups, "plates": n}


@app.post("/api/demo/malformed")
def demo_malformed():
    content = elispot.make_export("PLT-0666", 666).replace(b"\nC7,", b"\nC7-oops,").rsplit(b"# END", 1)[0]
    content += b"# END wells=96\n"
    _drop("PLT-0666-corrupt.csv", content)
    return {"ok": True}


@app.post("/api/demo/kill-worker")
def demo_kill(plate: str | None = None):
    """SIGKILL the worker that holds a live lease (mid-run). The supervisor starts a replacement."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, lease_owner FROM runs WHERE state='RUNNING' AND lease_until > now() "
                    "AND (%s::text IS NULL OR plate_id=%s) ORDER BY id LIMIT 1", (plate, plate))
        r = cur.fetchone()
    if not r:
        raise HTTPException(409, "no run in progress")
    pid = int(r["lease_owner"].rsplit("-", 1)[1])
    os.kill(pid, signal.SIGKILL)
    return {"killed": r["lease_owner"], "run_id": r["id"]}
