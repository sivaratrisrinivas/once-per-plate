"""Chaos script: real RabbitMQ, real Postgres, real worker processes. Prints PASS or FAIL per case.

Needs: Postgres at DATABASE_URL and RabbitMQ at AMQP_URL, plus commands to restart them
(OPP_PG_RESTART, OPP_MQ_STOP, OPP_MQ_START; defaults match the Makefile's local setup).
Usage: python scripts/chaos.py [case-name ...]"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DROP = os.environ.setdefault("OPP_DROP_DIR", "/tmp/opp-chaos-drop")
os.environ.setdefault("OPP_STABLE_SECONDS", "1.0")
os.environ.setdefault("OPP_LEASE_SECONDS", "3")

import pika  # noqa: E402

from opp import broker, config, db, elispot, ledger  # noqa: E402
from opp.db import connect  # noqa: E402

PG_RESTART = os.environ.get("OPP_PG_RESTART",
                            "/usr/lib/postgresql/17/bin/pg_ctl -D /workspace/opp-pgdata -m fast -w restart -l /workspace/opp-pg.log")
MQ_STOP = os.environ.get("OPP_MQ_STOP", "sudo rabbitmqctl stop_app")
MQ_START = os.environ.get("OPP_MQ_START", "sudo rabbitmqctl start_app")
API_PORT = 8078
PY = sys.executable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGDIR = os.path.join("/tmp", "opp-chaos-logs")


class Fail(AssertionError):
    pass


def check(cond, msg):
    if not cond:
        raise Fail(msg)


# ---------- process control ----------
class Fleet:
    def __init__(self):
        self.workers: list[subprocess.Popen] = []
        self.watcher: subprocess.Popen | None = None
        self.api: subprocess.Popen | None = None
        self.n = 0

    def _spawn(self, args, name, env_extra=None):
        env = dict(os.environ, **(env_extra or {}))
        self.n += 1
        log = open(os.path.join(LOGDIR, f"{self.n:03d}-{name}.log"), "w")
        return subprocess.Popen([PY, *args], env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)

    def start(self, workers=3, work=1.0, **env):
        e = {"OPP_WORK_SECONDS": str(work), **{k: str(v) for k, v in env.items()}}
        self.watcher = self._spawn(["-m", "opp.watcher"], "watcher", e)
        self.workers = [self._spawn(["-m", "opp.worker"], "worker", e) for _ in range(workers)]
        if self.api is None or self.api.poll() is not None:
            self.api = self._spawn(["-m", "uvicorn", "opp.api:app", "--port", str(API_PORT)], "api",
                                   {"OPP_DEMO": "0"})
        wait_for(lambda: consumers() >= workers, 20, "workers did not attach to RabbitMQ")
        wait_for(api_up, 20, "api did not start")

    def stop(self):
        for p in [*self.workers, self.watcher]:
            if p and p.poll() is None:
                p.kill()
        for p in [*self.workers, self.watcher]:
            if p:
                p.wait()
        self.workers, self.watcher = [], None

    def stop_all(self):
        self.stop()
        if self.api and self.api.poll() is None:
            self.api.kill()
            self.api.wait()

    def alive_workers(self):
        return [p for p in self.workers if p.poll() is None]


FLEET = Fleet()


def sh(cmd):
    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_for(fn, timeout, msg):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if fn():
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise Fail(msg)


def channel():
    conn = pika.BlockingConnection(broker.params())
    ch = conn.channel()
    broker.declare(ch)
    return conn, ch


def consumers():
    conn, ch = channel()
    try:
        return ch.queue_declare(config.RUN_QUEUE, passive=True).method.consumer_count
    finally:
        conn.close()


def parked_messages():
    conn, ch = channel()
    try:
        return ch.queue_declare(config.PARKED_QUEUE, passive=True).method.message_count
    finally:
        conn.close()


def api_up():
    return urllib.request.urlopen(f"http://127.0.0.1:{API_PORT}/health", timeout=2).status == 200


def http_post(path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{API_PORT}{path}", data=json.dumps(body or {}).encode(),
                                 method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, {}


# ---------- data helpers ----------
def q(sql, *args):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else None


def one(sql, *args):
    rows = q(sql, *args)
    return list(rows[0].values())[0] if rows else None


def export(n, **kw):
    return elispot.make_export(f"PLT-{n:04d}", n, **kw)


def drop(name, content):
    tmp = os.path.join(DROP, name + ".part")
    with open(tmp, "wb") as f:
        f.write(content)
    os.replace(tmp, os.path.join(DROP, name))


def states():
    return {r["state"]: r["n"] for r in q("SELECT state, count(*) AS n FROM runs GROUP BY state")}


def settled():
    """No run still in flight and no file waiting in the drop folder."""
    pending = [n for n in os.listdir(DROP) if os.path.isfile(os.path.join(DROP, n))]
    return not pending and not one("SELECT count(*) FROM runs WHERE state IN ('RECEIVED','QUEUED','RUNNING')")


def wait_settled(timeout=60):
    wait_for(settled, timeout, f"not settled after {timeout}s: {states()}")


def invariants():
    """Global checks after every case."""
    distinct_bytes = one("SELECT count(DISTINCT sha256) FROM arrivals")
    runs = one("SELECT count(*) FROM runs")
    check(runs == distinct_bytes, f"runs {runs} != distinct file contents {distinct_bytes}")
    written = one("SELECT count(*) FROM runs WHERE state='WRITTEN'")
    rows = one("SELECT count(*) FROM labsheet_rows")
    check(rows == 96 * written, f"labsheet rows {rows} != 96 x {written} written runs")
    orphans = one("SELECT count(*) FROM runs r WHERE NOT EXISTS (SELECT 1 FROM arrivals a WHERE a.run_id=r.id)")
    check(orphans == 0, "run without an arrival")
    check(ledger.rebuild_states() == ledger.run_states(), "event log replay disagrees with runs table")
    proposed_twice = one("SELECT count(*) FROM (SELECT run_id FROM events WHERE kind='proposed' "
                         "GROUP BY run_id HAVING count(*) > 1) x")
    check(proposed_twice == 0, "a run produced two proposals")


def fresh(workers=3, work=1.0, **env):
    FLEET.stop()
    db.reset()
    p = broker.Publisher()
    p.purge()
    shutil.rmtree(DROP, ignore_errors=True)
    os.makedirs(DROP)
    FLEET.start(workers=workers, work=work, **env)


def wait_state(run_id, state, timeout=30):
    wait_for(lambda: one("SELECT state FROM runs WHERE id=%s", run_id) == state, timeout,
             f"run {run_id} never reached {state} (is {one('SELECT state FROM runs WHERE id=%s', run_id)})")


# ---------- cases ----------
def case_same_file_twice():
    """Same export dropped twice, then the same bytes under a new name: one run."""
    fresh()
    c = export(1)
    drop("PLT-0001.csv", c)
    wait_settled()
    drop("PLT-0001.csv", c)
    drop("plate1 (1).csv", c)
    wait_settled()
    check(one("SELECT count(*) FROM runs") == 1, "expected 1 run")
    check(one("SELECT count(*) FROM arrivals") == 3, "expected 3 arrivals")
    check(one("SELECT count(*) FROM arrivals WHERE outcome='DUPLICATE'") == 2, "expected 2 linked duplicates")
    check(one("SELECT attempts FROM runs") == 1, "QC ran more than once")
    return "3 arrivals (2 names) -> 1 run, QC ran once"


def case_half_written():
    """A slow writer: no run until the file is complete. With a sidecar, even long pauses are safe."""
    fresh()
    c = export(2)
    path = os.path.join(DROP, "PLT-0002.csv")
    chunks = [c[i:i + len(c) // 6 + 1] for i in range(0, len(c), len(c) // 6 + 1)]
    with open(path, "wb") as f:
        for ch in chunks:
            f.write(ch)
            f.flush()
            time.sleep(0.5)  # shorter than the stable window
            check(one("SELECT count(*) FROM runs") == 0, "a run started on a half-written file")
    wait_settled()
    check(one("SELECT sha256 FROM runs") == hashlib.sha256(c).hexdigest(), "run is not keyed on the full file")
    c3 = export(3)
    p3 = os.path.join(DROP, "PLT-0003.csv")
    with open(p3 + ".sha256", "w") as f:
        f.write(hashlib.sha256(c3).hexdigest())
    with open(p3, "wb") as f:
        f.write(c3[:len(c3) // 2])
        f.flush()
        time.sleep(2.5)  # longer than the stable window: only the sidecar holds it back
        check(one("SELECT count(*) FROM runs") == 1, "sidecar did not hold a stalled half file")
        f.write(c3[len(c3) // 2:])
    wait_settled()
    check(one("SELECT count(*) FROM runs") == 2 and one("SELECT count(*) FROM runs WHERE state='PARKED'") == 0,
          "expected 2 clean runs")
    return "no run while writing; stalled half file held by checksum sidecar"


def case_reexport_changed_well():
    """Approved plate re-exported with one well changed: new version, needs its own approval."""
    fresh()
    drop("PLT-0004.csv", export(4))
    wait_settled()
    r1 = one("SELECT id FROM runs")
    s, _ = http_post(f"/api/runs/{r1}/approve", {"approver": "analyst"})
    check(s == 200, "approve failed")
    from datetime import datetime, timezone
    later = datetime(2026, 10, 13, 1, 0, tzinfo=timezone.utc)
    drop("PLT-0004-reexport.csv", export(4, tweak={"B5": 41}, exported_at=later))
    wait_settled()
    v2 = q("SELECT id, version, supersedes_run_id, state FROM runs WHERE version=2")[0]
    check(v2["supersedes_run_id"] == r1, "v2 not linked to v1")
    check(v2["state"] == "PROPOSED", f"v2 should wait for approval, is {v2['state']}")
    check(one("SELECT count(*) FROM labsheet_rows") == 96, "v2 wrote rows without approval")
    http_post(f"/api/runs/{v2['id']}/approve", {"approver": "analyst"})
    check(one("SELECT count(*) FROM labsheet_rows WHERE run_id=%s", v2["id"]) == 96, "v2 rows missing")
    return "v2 flagged 'supersedes v1', waited for its own approval"


def case_kill_worker_mid_run():
    """kill -9 the worker holding the run. RabbitMQ redelivers; the next worker takes over after the lease."""
    fresh(workers=2, work=3.0)
    drop("PLT-0005.csv", export(5))
    wait_for(lambda: one("SELECT state FROM runs") == "RUNNING", 15, "run never started")
    owner = one("SELECT lease_owner FROM runs")
    pid = int(owner.rsplit("-", 1)[1])
    os.kill(pid, signal.SIGKILL)
    wait_settled(40)
    r = q("SELECT state, attempts FROM runs")[0]
    check(r["state"] == "PROPOSED", f"run is {r['state']}")
    check(r["attempts"] == 2, f"attempts {r['attempts']}")
    check(one("SELECT count(*) FROM events WHERE kind='resumed'") == 1, "no takeover recorded")
    check(len(FLEET.alive_workers()) == 1, "expected exactly one dead worker")
    return f"killed {owner}; redelivered, resumed on attempt 2, one proposal"


def case_crash_before_ack():
    """Worker commits its result, then dies before acking. The redelivered message is a no-op."""
    flag = "/tmp/opp-exit-before-ack"
    open(flag, "w").close()
    fresh(workers=2, work=0.5, OPP_CHAOS_EXIT_BEFORE_ACK=flag)
    drop("PLT-0006.csv", export(6))
    wait_settled()
    wait_for(lambda: one("SELECT count(*) FROM events WHERE kind='duplicate_delivery'") >= 1, 15,
             "redelivery never seen")
    check(not os.path.exists(flag), "crash hook did not fire")
    check(one("SELECT attempts FROM runs") == 1, "QC ran twice")
    return "result committed, ack lost, redelivery ignored"


def case_duplicate_messages():
    """At-least-once delivery: the same run id published 5 extra times."""
    fresh(workers=3, work=1.0)
    drop("PLT-0007.csv", export(7))
    wait_for(lambda: one("SELECT count(*) FROM runs") == 1, 10, "no run")
    rid = one("SELECT id FROM runs")
    p = broker.Publisher()
    for _ in range(5):
        p.publish(rid, reason="chaos-duplicate")
    wait_settled()
    wait_for(lambda: one("SELECT count(*) FROM events WHERE kind='duplicate_delivery'") == 5, 15,
             f"saw {one('SELECT count(*) FROM events WHERE kind=%s', 'duplicate_delivery')} duplicate deliveries")
    check(one("SELECT attempts FROM runs") == 1, "QC ran more than once")
    return "6 messages for one run -> 1 QC run, 5 no-ops"


def case_broker_restart():
    """RabbitMQ stops with unacked messages in flight, and files keep arriving while it is down."""
    fresh(workers=2, work=2.0)
    for i in range(10, 16):
        drop(f"PLT-{i:04d}.csv", export(i))
    wait_for(lambda: one("SELECT count(*) FROM runs WHERE state='RUNNING'") >= 2, 15, "no runs in flight")
    sh(MQ_STOP)
    try:
        for i in range(16, 19):
            drop(f"PLT-{i:04d}.csv", export(i))  # arrive while the broker is down
        wait_for(lambda: one("SELECT count(*) FROM runs") == 9, 15, "arrivals not recorded while broker down")
        time.sleep(1)
    finally:
        sh(MQ_START)
    wait_settled(60)
    check(states() == {"PROPOSED": 9}, f"states {states()}")
    check(one("SELECT count(*) FROM events WHERE kind='proposed'") == 9, "proposal count")
    return "9 plates (3 arrived while down) -> 9 runs, 9 proposals"


def case_postgres_restart():
    """Postgres restarts in the middle of a batch."""
    fresh(workers=3, work=1.0)
    for i in range(20, 28):
        drop(f"PLT-{i:04d}.csv", export(i))
    wait_for(lambda: one("SELECT count(*) FROM runs WHERE state='RUNNING'") >= 1, 15, "no run started")
    sh(PG_RESTART)
    for i in range(28, 31):
        drop(f"PLT-{i:04d}.csv", export(i))
    wait_settled(60)
    check(states() == {"PROPOSED": 11}, f"states {states()}")
    return "restart mid-batch; 11 plates -> 11 proposals"


def case_burst_50():
    """50 plates plus 10 duplicate re-syncs land at once."""
    fresh(workers=4, work=0.3)
    t0 = time.time()
    for i in range(100, 150):
        drop(f"PLT-{i:04d}.csv", export(i))
    for i in range(100, 150, 5):
        drop(f"PLT-{i:04d}-resync.csv", export(i))
    wait_settled(120)
    dt = time.time() - t0
    check(one("SELECT count(*) FROM arrivals") == 60, "arrival count")
    check(one("SELECT count(*) FROM runs") == 50, f"runs {one('SELECT count(*) FROM runs')}")
    check(states() == {"PROPOSED": 50}, f"states {states()}")
    check(one("SELECT max(attempts) FROM runs") == 1, "a plate ran twice")
    return f"60 files -> 50 runs, every plate once ({dt:.0f}s, 4 workers)"


def case_approve_twice():
    """Approve clicked 10 times at once (double clicks, two tabs, a retrying client)."""
    fresh()
    drop("PLT-0200.csv", export(200))
    wait_settled()
    rid = one("SELECT id FROM runs")
    with ThreadPoolExecutor(10) as ex:
        res = list(ex.map(lambda _: http_post(f"/api/runs/{rid}/approve", {"approver": "analyst"}), range(10)))
    check(all(s == 200 for s, _ in res), f"statuses {[s for s, _ in res]}")
    check(sum(1 for _, b in res if not b.get("repeat")) == 1, "more than one approval applied")
    check(one("SELECT count(*) FROM labsheet_rows") == 96, "row count")
    check(one("SELECT count(*) FROM events WHERE kind='approved'") == 1, "approval events")
    return "10 concurrent approvals -> 1 applied, 96 rows"


def case_reject_then_redrop():
    """Rejected run; the same file is dropped again. It is linked, not run again."""
    fresh()
    c = export(201)
    drop("PLT-0201.csv", c)
    wait_settled()
    rid = one("SELECT id FROM runs")
    s, _ = http_post(f"/api/runs/{rid}/reject", {"approver": "qa lead", "reason": "wrong plate map"})
    check(s == 200, "reject failed")
    drop("PLT-0201.csv", c)
    wait_settled()
    check(one("SELECT count(*) FROM runs") == 1, "re-drop started a new run")
    check(one("SELECT state FROM runs") == "REJECTED", "state changed")
    check(one("SELECT count(*) FROM labsheet_rows") == 0, "rows written for a rejected run")
    s, _ = http_post(f"/api/runs/{rid}/approve", {"approver": "someone"})
    check(s == 409, "rejected run could be approved")
    return "re-drop linked to the rejected run; 0 rows; approve now 409"


def case_writeback_timeout():
    """Rows commit but the writer never hears back, so it retries. Then the sweeper retries too."""
    fresh()
    drop("PLT-0202.csv", export(202))
    wait_settled()
    rid = one("SELECT id FROM runs")
    check(ledger.approve(rid, "analyst")["ok"], "approve")
    try:
        ledger.write_back(rid, lose_ack=True)
        raise Fail("hook did not fire")
    except ledger.WriteAckLost:
        pass
    again = ledger.write_back(rid)
    time.sleep(3)  # sweeper pass
    check(again["written"] == 0, "retry wrote rows again")
    check(one("SELECT count(*) FROM labsheet_rows") == 96, "row count")
    # Approved but never written (writer died): the sweeper finishes it once.
    drop("PLT-0203.csv", export(203))
    wait_settled()
    r2 = one("SELECT id FROM runs WHERE plate_id='PLT-0203'")
    ledger.approve(r2, "analyst")
    wait_state(r2, "WRITTEN", 15)
    check(one("SELECT count(*) FROM labsheet_rows") == 192, "row count after sweeper")
    return "retried write is a no-op; sweeper finished an orphaned approval"


def case_two_workers_race():
    """20 workers try to claim the same run at the same instant."""
    FLEET.stop()
    db.reset()
    broker.Publisher().purge()
    shutil.rmtree(DROP, ignore_errors=True)
    os.makedirs(DROP)
    r = ledger.ingest(export(204), "PLT-0204.csv", "chaos")
    rid = r["run_id"]
    gate = threading.Barrier(20)

    def go(i):
        gate.wait()
        return ledger.claim(rid, f"racer-{i}", 30)

    with ThreadPoolExecutor(20) as ex:
        res = list(ex.map(go, range(20)))
    check(res.count("claimed") == 1, f"claims: {res.count('claimed')}")
    winner = one("SELECT lease_owner FROM runs")
    loser = next(f"racer-{i}" for i in range(20) if f"racer-{i}" != winner)
    check(not ledger.finalize(rid, loser, {"tally": {}, "wells": [], "calls": {}}), "loser could finalize")
    check(ledger.finalize(rid, winner, elispot.qc(elispot.parse(export(204)))), "winner could not finalize")
    FLEET.start()
    return f"1 of 20 claims won ({winner}); the losers cannot write"


def case_out_of_order():
    """v2 arrives while v1 is still running; then an older export shows up late."""
    from datetime import datetime, timezone
    fresh(workers=2, work=3.0)
    t = lambda h: datetime(2026, 10, 12, h, 0, tzinfo=timezone.utc)  # noqa: E731
    drop("PLT-0300.csv", export(300, exported_at=t(18)))
    wait_for(lambda: one("SELECT state FROM runs") == "RUNNING", 15, "v1 never started")
    drop("PLT-0300-v2.csv", export(300, exported_at=t(19), tweak={"C4": 30}))
    wait_settled(40)
    drop("PLT-0300-old.csv", export(300, exported_at=t(17), tweak={"C4": 1}))
    wait_settled(20)
    rows = {r["version"]: r for r in q("SELECT version, state FROM runs ORDER BY version")}
    check(rows[1]["state"] == "SUPERSEDED", f"v1 is {rows[1]['state']}")
    check(rows[2]["state"] == "PROPOSED", f"v2 is {rows[2]['state']}")
    check(rows[3]["state"] == "SUPERSEDED", f"late old export is {rows[3]['state']}")
    check(one("SELECT count(*) FROM events WHERE kind='late_finish_discarded'") == 1, "v1 result not discarded")
    check(one("SELECT count(*) FROM runs WHERE state='PROPOSED'") == 1, "more than one approvable version")
    return "v1 result discarded, older late export kept for audit only; 1 approvable version"


def case_malformed_parked():
    """A corrupt export goes to the parked queue once, and the workers keep going."""
    fresh(workers=2, work=0.3)
    bad = export(400).replace(b"\nC7,", b"\nC7-oops,")
    drop("PLT-0400-corrupt.csv", bad)
    drop("garbage.bin", os.urandom(4000))
    drop("PLT-0401.csv", export(401))
    wait_settled()
    time.sleep(1)
    check(one("SELECT count(*) FROM runs WHERE state='PARKED'") == 2, f"states {states()}")
    check(one("SELECT max(attempts) FROM runs WHERE state='PARKED'") == 1, "parked file retried")
    check(parked_messages() == 2, f"parked queue has {parked_messages()}")
    check(one("SELECT state FROM runs WHERE plate_id='PLT-0401'") == "PROPOSED", "good file blocked")
    check(len(FLEET.alive_workers()) == 2, "a worker crashed")
    return "2 bad files -> parked queue, 1 attempt each; good file still ran"


def case_ledger_replay():
    """Mixed workload, then rebuild every run's state from the append-only event log."""
    fresh(workers=3, work=0.3)
    for i in range(500, 512):
        drop(f"PLT-{i:04d}.csv", export(i))
    drop("PLT-0500-again.csv", export(500))
    wait_settled()
    ids = [r["id"] for r in q("SELECT id FROM runs ORDER BY id")]
    for rid in ids[:5]:
        http_post(f"/api/runs/{rid}/approve", {"approver": "analyst"})
    for rid in ids[5:8]:
        http_post(f"/api/runs/{rid}/reject", {"approver": "qa lead", "reason": "chaos"})
    rebuilt, table = ledger.rebuild_states(), ledger.run_states()
    check(rebuilt == table, "replay mismatch")
    check(sorted(set(table.values())) == ["PROPOSED", "REJECTED", "WRITTEN"], f"states {set(table.values())}")
    return f"{len(table)} runs rebuilt from {one('SELECT count(*) FROM events')} events, all match"


CASES = [case_same_file_twice, case_half_written, case_reexport_changed_well, case_kill_worker_mid_run,
         case_crash_before_ack, case_duplicate_messages, case_broker_restart, case_postgres_restart,
         case_burst_50, case_approve_twice, case_reject_then_redrop, case_writeback_timeout,
         case_two_workers_race, case_out_of_order, case_malformed_parked, case_ledger_replay]


def main():
    os.makedirs(LOGDIR, exist_ok=True)
    db.init_schema()
    wanted = sys.argv[1:]
    results = []
    t_all = time.time()
    try:
        for fn in CASES:
            name = fn.__name__[5:]
            if wanted and name not in wanted:
                continue
            t0 = time.time()
            try:
                detail = fn()
                wait_settled(30) if FLEET.watcher else None
                invariants()
                ok = True
            except Exception as e:  # noqa: BLE001
                ok, detail = False, f"{type(e).__name__}: {e}"
            dt = time.time() - t0
            results.append({"case": name, "pass": ok, "detail": detail, "seconds": round(dt, 1),
                            "doc": (fn.__doc__ or "").strip()})
            print(f"{'PASS' if ok else 'FAIL'}  {name:<24} {detail}  [{dt:.1f}s]", flush=True)
    finally:
        FLEET.stop_all()
    n_pass = sum(r["pass"] for r in results)
    print(f"\n{n_pass}/{len(results)} passed in {time.time() - t_all:.0f}s "
          f"(RabbitMQ + Postgres, real worker processes)")
    if not wanted:
        out = {"passed": n_pass, "total": len(results), "ran_at": time.strftime("%Y-%m-%d %H:%M %Z"),
               "environment": env_line(), "cases": results}
        json.dump(out, open(os.path.join(ROOT, "docs", "chaos-results.json"), "w"), indent=2)
    sys.exit(0 if n_pass == len(results) else 1)


def env_line():
    try:
        pg = one("SHOW server_version")
    except Exception:
        pg = "?"
    try:
        mq = subprocess.run("sudo rabbitmqctl version", shell=True, capture_output=True, text=True).stdout.strip()
    except Exception:
        mq = "?"
    return f"PostgreSQL {pg}, RabbitMQ {mq or '?'}, Python {sys.version.split()[0]}"


if __name__ == "__main__":
    main()
