import logging
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class EtlPostgresHook(BaseHook):
    """Hook for interacting with Postgres sources in the ETL pipeline."""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if self._conn is None or self._conn.closed:
            conn_details = BaseHook.get_connection(self.conn_id)
            self._conn = psycopg2.connect(
                host=conn_details.host,
                port=conn_details.port or 5432,
                dbname=conn_details.schema,
                user=conn_details.login,
                password=conn_details.password,
                connect_timeout=10,
            )
        return self._conn

    def extract_data(self, schema: str, table: str, chunk_size: int = 10000):
        conn = self.get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f'SELECT * FROM {schema}.{table}')
            columns = [desc[0] for desc in cur.description]
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield [dict(row) for row in rows], columns
        log.info(f"Finished extracting from {schema}.{table}")

    def get_row_count(self, schema: str, table: str) -> int:
        conn = self.get_conn()
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {schema}.{table}')
            return cur.fetchone()[0]

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
