"""QC worker. Consumes run ids from RabbitMQ with manual ack and prefetch 1.

A message is only a hint; the ledger row decides. A redelivered or duplicated message finds the
run already claimed or finished and becomes a no-op. A crashed worker's message comes back
(unacked) and the next worker takes over once the dead worker's lease runs out."""
from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time

import pika
import psycopg

from . import broker, config, elispot, ledger

WORKER = os.environ.get("OPP_WORKER_ID") or f"worker-{os.getpid()}"


def log(*a):
    print(time.strftime("%H:%M:%S"), WORKER, *a, flush=True)


def process(run_id: int) -> str:
    deadline = time.time() + config.LEASE_SECONDS + 3
    while True:
        status = ledger.claim(run_id, WORKER, config.LEASE_SECONDS)
        if status != "busy" or time.time() > deadline:
            break
        time.sleep(0.4)  # another worker holds a live lease: wait to see if it dies or finishes
    if status in ("done", "missing", "busy"):
        if status != "missing":
            ledger.note_duplicate_delivery(run_id, WORKER, status)
        return status
    content = ledger.load_content(run_id)
    rows = elispot.parse(content)  # raises MalformedExport
    stop = threading.Event()

    def heartbeat():
        while not stop.wait(config.LEASE_SECONDS / 3):
            try:
                ledger.renew(run_id, WORKER, config.LEASE_SECONDS)
            except psycopg.OperationalError:
                pass

    hb = threading.Thread(target=heartbeat, daemon=True)
    hb.start()
    try:
        time.sleep(config.WORK_SECONDS)  # stands in for a long analysis step
        result = elispot.qc(rows)
    finally:
        stop.set()
    ok = ledger.finalize(run_id, WORKER, result)
    return "proposed" if ok else "discarded"


def on_message(ch, method, props, body):
    run_id = json.loads(body)["run_id"]
    try:
        outcome = process(run_id)
        flag = os.environ.get("OPP_CHAOS_EXIT_BEFORE_ACK")
        if flag and outcome == "proposed" and os.path.exists(flag):
            os.remove(flag)  # chaos hook: die after the result is committed, before the ack
            os._exit(137)
        log(f"run {run_id}: {outcome}{' (redelivered)' if method.redelivered else ''}")
        ch.basic_ack(method.delivery_tag)
    except elispot.MalformedExport as e:
        ledger.park(run_id, WORKER, str(e))
        log(f"run {run_id}: parked ({e})")
        ch.basic_reject(method.delivery_tag, requeue=False)  # dead-lettered to opp.parked
    except psycopg.OperationalError as e:
        log(f"run {run_id}: database unavailable, requeue ({str(e).splitlines()[0][:80]})")
        time.sleep(1)
        ch.basic_nack(method.delivery_tag, requeue=True)


def main():
    log("starting")
    while True:
        try:
            conn = pika.BlockingConnection(broker.params())
            ch = conn.channel()
            broker.declare(ch)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(config.RUN_QUEUE, on_message)
            log("consuming", config.RUN_QUEUE)
            ch.start_consuming()
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:  # broker restart or network drop: reconnect
            log("broker connection lost, retrying:", type(e).__name__)
            time.sleep(1)


if __name__ == "__main__":
    main()
