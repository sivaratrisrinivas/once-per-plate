"""Folder watcher plus sweeper, one loop.

Watcher: a file in the drop folder counts as ready when a matching .sha256 sidecar exists, or when
its size and mtime have not changed for OPP_STABLE_SECONDS. Ready files are ingested and moved to
drop/done/. A half-written export is never read as a run.

Sweeper: publishes runs that were recorded but never reached the queue (broker was down), nudges
runs whose worker lease expired, and finishes write-backs that were approved but not written."""
from __future__ import annotations

import hashlib
import os
import shutil
import time

import psycopg

from . import broker, config, ledger
from .db import connect

SKIP_SUFFIXES = (".part", ".tmp", ".sha256", ".crdownload")


def log(*a):
    print(time.strftime("%H:%M:%S"), "watcher", *a, flush=True)


def ingest_and_publish(content: bytes, filename: str, source: str, pub: broker.Publisher) -> dict:
    r = ledger.ingest(content, filename, source)
    if r["publish"]:
        if pub.publish(r["run_id"]):
            ledger.mark_published(r["run_id"])
    return r


class Watcher:
    def __init__(self, folder: str, pub: broker.Publisher):
        self.folder = folder
        self.pub = pub
        self.seen: dict[str, tuple[int, int, float]] = {}
        os.makedirs(os.path.join(folder, "done"), exist_ok=True)

    def _sidecar_ok(self, path: str, content: bytes) -> bool | None:
        side = path + ".sha256"
        if not os.path.exists(side):
            return None
        want = open(side).read().split()[0].strip().lower() if os.path.getsize(side) else ""
        return want == hashlib.sha256(content).hexdigest()

    def scan(self) -> int:
        n = 0
        now = time.time()
        try:
            names = sorted(os.listdir(self.folder))
        except FileNotFoundError:
            return 0
        for name in names:
            path = os.path.join(self.folder, name)
            if name.startswith(".") or name.endswith(SKIP_SUFFIXES) or not os.path.isfile(path):
                continue
            st = os.stat(path)
            sig = (st.st_size, st.st_mtime_ns)
            prev = self.seen.get(path)
            if prev is None or prev[:2] != sig:
                self.seen[path] = (*sig, now)
                if not os.path.exists(path + ".sha256"):
                    continue
            content = open(path, "rb").read()
            side = self._sidecar_ok(path, content)
            stable = now - self.seen[path][2] >= config.STABLE_SECONDS
            if side is False or (side is None and not stable) or st.st_size == 0:
                continue
            try:
                r = ingest_and_publish(content, name, "folder", self.pub)
            except psycopg.OperationalError:
                return n  # Postgres down: leave the file, try again next scan
            log(f"{name}: {r['outcome']} -> run {r['run_id']}")
            dest = os.path.join(self.folder, "done", f"{int(now * 1000)}-{name}")
            shutil.move(path, dest)
            if os.path.exists(path + ".sha256"):
                os.remove(path + ".sha256")
            self.seen.pop(path, None)
            n += 1
        return n


def sweep(pub: broker.Publisher) -> None:
    with connect(retries=1) as conn, conn.cursor() as cur:
        cur.execute("""SELECT id, state FROM runs WHERE
              (state='RECEIVED' AND updated_at < now() - interval '2 seconds'
                 AND (published_at IS NULL OR published_at < now() - interval '5 seconds'))
           OR (state='QUEUED' AND published_at < now() - interval '120 seconds')
           OR (state='RUNNING' AND lease_until < now() - interval '1 second'
                 AND (published_at IS NULL OR published_at < lease_until))
           ORDER BY id LIMIT 200""")
        todo = cur.fetchall()
        cur.execute("SELECT id FROM runs WHERE state='APPROVED' AND updated_at < now() - interval '2 seconds'")
        approved = [r["id"] for r in cur.fetchall()]
    for r in todo:
        if pub.publish(r["id"], reason="sweeper"):
            ledger.mark_published(r["id"])
    for rid in approved:
        ledger.write_back(rid, actor="sweeper")


def main():
    pub = broker.Publisher()
    w = Watcher(config.DROP_DIR, pub)
    log("watching", config.DROP_DIR, "stable", config.STABLE_SECONDS, "s")
    last_sweep = 0.0
    while True:
        try:
            w.scan()
            if time.time() - last_sweep > 1.0:
                sweep(pub)
                last_sweep = time.time()
        except psycopg.OperationalError:
            log("database unavailable, retrying")
            time.sleep(1)
        except Exception as e:  # never let one bad file or a broker blip stop the watcher
            log("error, retrying:", type(e).__name__, str(e)[:120])
            time.sleep(1)
        time.sleep(0.2)


if __name__ == "__main__":
    main()
