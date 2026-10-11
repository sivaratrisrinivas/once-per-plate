"""RabbitMQ plumbing: one durable work queue, manual acks, prefetch 1, and a dead-letter
exchange that routes rejected (malformed) runs to a parked queue instead of a retry loop."""
from __future__ import annotations

import json
import threading
import time

import pika

from . import config


def params() -> pika.URLParameters:
    p = pika.URLParameters(config.AMQP_URL)
    p.heartbeat = 30
    p.blocked_connection_timeout = 10
    p.connection_attempts = 1
    p.socket_timeout = 3
    return p


def declare(ch) -> None:
    ch.exchange_declare(config.DLX, "direct", durable=True)
    ch.queue_declare(config.PARKED_QUEUE, durable=True)
    ch.queue_bind(config.PARKED_QUEUE, config.DLX, routing_key=config.PARKED_QUEUE)
    ch.queue_declare(config.RUN_QUEUE, durable=True, arguments={
        "x-dead-letter-exchange": config.DLX, "x-dead-letter-routing-key": config.PARKED_QUEUE})


class Publisher:
    """Thread-safe publisher with confirms. publish() returns False if the broker is down;
    the ledger keeps the run RECEIVED and the sweeper publishes it later."""

    def __init__(self):
        self._lock = threading.Lock()
        self._conn = None
        self._ch = None

    def _channel(self):
        if self._conn is None or self._conn.is_closed or self._ch is None or self._ch.is_closed:
            self._conn = pika.BlockingConnection(params())
            self._ch = self._conn.channel()
            self._ch.confirm_delivery()
            declare(self._ch)
        return self._ch

    def publish(self, run_id: int, reason: str = "arrival") -> bool:
        with self._lock:
            for _ in range(2):
                try:
                    ch = self._channel()
                    ch.basic_publish("", config.RUN_QUEUE, json.dumps({"run_id": run_id, "reason": reason}),
                                     pika.BasicProperties(delivery_mode=2, content_type="application/json"),
                                     mandatory=True)
                    return True
                except Exception:
                    self._conn = None
                    time.sleep(0.2)
            return False

    def counts(self) -> dict:
        with self._lock:
            try:
                ch = self._channel()
                q = ch.queue_declare(config.RUN_QUEUE, passive=True)
                p = ch.queue_declare(config.PARKED_QUEUE, passive=True)
                return {"ready": q.method.message_count, "consumers": q.method.consumer_count,
                        "parked": p.method.message_count, "up": True}
            except Exception:
                self._conn = None
                return {"up": False}

    def purge(self) -> None:
        with self._lock:
            ch = self._channel()
            ch.queue_purge(config.RUN_QUEUE)
            ch.queue_purge(config.PARKED_QUEUE)
