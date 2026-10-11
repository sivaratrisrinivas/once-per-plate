import os
import time

import psycopg
from psycopg.rows import dict_row

from . import config


def connect(retries: int = 60, delay: float = 0.5) -> psycopg.Connection:
    """Open a connection, retrying while Postgres is down or restarting."""
    last = None
    for _ in range(retries):
        try:
            return psycopg.connect(config.DATABASE_URL, row_factory=dict_row, connect_timeout=3)
        except psycopg.OperationalError as e:
            last = e
            time.sleep(delay)
    raise last


def init_schema() -> None:
    sql = open(os.path.join(os.path.dirname(__file__), "schema.sql")).read()
    with connect() as conn:
        conn.execute("SELECT pg_advisory_lock(424242)")
        conn.execute(sql)
        conn.execute("SELECT pg_advisory_unlock(424242)")


def reset() -> None:
    with connect() as conn:
        conn.execute("TRUNCATE labsheet_rows, events, arrivals, runs, blobs RESTART IDENTITY CASCADE")
