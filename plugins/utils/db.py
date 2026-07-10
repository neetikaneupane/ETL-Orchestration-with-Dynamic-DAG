"""
db.py
=====
Shared database connection utility for the ETL pipeline.
"""

from contextlib import contextmanager
from typing import Generator

import psycopg2
from airflow.hooks.base import BaseHook


def get_pg_conn(conn_id: str = "pipeline_config_db"):
    """Create a new psycopg2 connection from an Airflow connection."""
    conn_details = BaseHook.get_connection(conn_id)
    extra = conn_details.extra_dejson if conn_details.extra else {}
    sslmode = extra.get("sslmode", "prefer")
    return psycopg2.connect(
        host=conn_details.host,
        port=conn_details.port or 5432,
        dbname=conn_details.schema,
        user=conn_details.login,
        password=conn_details.password,
        sslmode=sslmode,
    )


@contextmanager
def pg_cursor(conn_id: str = "pipeline_config_db") -> Generator:
    """Context manager that provides a cursor and handles commit/close."""
    conn = get_pg_conn(conn_id)
    try:
        with conn.cursor() as cur:
            yield cur
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
