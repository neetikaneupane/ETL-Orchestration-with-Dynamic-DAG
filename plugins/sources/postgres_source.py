import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from hooks.etl_postgres_hook import EtlPostgresHook
from .base_source import BaseSource

log = logging.getLogger(__name__)


class PostgresSource(BaseSource):
    """Postgres data source with optional incremental extraction.

    To enable incremental mode, include in source_config:
        "incremental_column": "updated_at"   # column to track
        "watermark_table": "pipeline_config.watermarks"  # where watermark is stored
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.conn_id = config.get("conn_id", "pipeline_config_db")
        self.schema = config.get("schema", "public")
        self.table = config["table"]
        self.incremental_column = config.get("incremental_column")
        self.watermark_table = config.get("watermark_table", "pipeline_config.watermarks")
        self._hook: Optional[EtlPostgresHook] = None

    @property
    def hook(self) -> EtlPostgresHook:
        if self._hook is None:
            self._hook = EtlPostgresHook(self.conn_id)
        return self._hook

    def _get_watermark(self) -> Optional[datetime]:
        """Fetch the last extracted watermark for this source table."""
        if not self.incremental_column:
            return None
        conn = self.hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT watermark_value FROM pipeline_config.watermarks
                WHERE source_table = %s
            """, (f"{self.schema}.{self.table}",))
            row = cur.fetchone()
        return row[0] if row else None

    def _set_watermark(self, value: datetime):
        """Update or insert the watermark after extraction."""
        if not self.incremental_column:
            return
        conn = self.hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_config.watermarks (source_table, watermark_value)
                VALUES (%s, %s)
                ON CONFLICT (source_table)
                DO UPDATE SET watermark_value = EXCLUDED.watermark_value,
                              updated_at = NOW()
            """, (f"{self.schema}.{self.table}", value))
            conn.commit()

    def _build_query(self) -> Tuple[str, List[Any]]:
        """Build the extraction query, with optional incremental filter."""
        base = f"SELECT * FROM {self.schema}.{self.table}"
        params = []

        if self.incremental_column:
            watermark = self._get_watermark()
            if watermark:
                base += f" WHERE {self.incremental_column} > %s"
                params.append(watermark)
            base += f" ORDER BY {self.incremental_column} ASC"

        return base, params

    def extract(self, context: Optional[dict] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
        conn = self.hook.get_conn()
        query, params = self._build_query()

        log.info(f"Extracting from {self.schema}.{self.table} "
                 f"{'(incremental)' if self.incremental_column else '(full)'}")
        if self.incremental_column and params:
            log.info(f"Watermark: {params[0]}")

        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            all_rows = cur.fetchall()
            rows = [dict(zip(columns, row)) for row in all_rows]

        if self.incremental_column and rows:
            max_val = max(row[self.incremental_column] for row in rows)
            self._set_watermark(max_val)
            log.info(f"Updated watermark to {max_val}")

        log.info(f"Extracted {len(rows)} rows from {self.schema}.{self.table}")
        return rows, columns

    def get_row_count(self) -> int:
        return self.hook.get_row_count(self.schema, self.table)

    def close(self):
        if self._hook:
            self._hook.close()
