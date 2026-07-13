import logging

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
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
            extra = conn_details.extra_dejson if conn_details.extra else {}
            sslmode = extra.get("sslmode", "prefer")
            sslrootcert = extra.get("sslrootcert")
            sslcert = extra.get("sslcert")
            sslkey = extra.get("sslkey")
            connect_kwargs = {
                "host": conn_details.host,
                "port": conn_details.port or 5432,
                "dbname": conn_details.schema,
                "user": conn_details.login,
                "password": conn_details.password,
                "connect_timeout": 10,
                "sslmode": sslmode,
            }
            if sslrootcert:
                connect_kwargs["sslrootcert"] = sslrootcert
            if sslcert:
                connect_kwargs["sslcert"] = sslcert
            if sslkey:
                connect_kwargs["sslkey"] = sslkey
            self._conn = psycopg2.connect(**connect_kwargs)
        return self._conn

    def extract_data(self, schema: str, table: str, chunk_size: int = 10000):
        conn = self.get_conn()
        query = sql.SQL("SELECT * FROM {schema}.{table}").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield [dict(row) for row in rows], columns
        log.info(f"Finished extracting from {schema}.{table}")

    def get_row_count(self, schema: str, table: str) -> int:
        conn = self.get_conn()
        query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
