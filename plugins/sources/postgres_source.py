import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from hooks.etl_postgres_hook import EtlPostgresHook
from .base_source import BaseSource

log = logging.getLogger(__name__)


class PostgresSource(BaseSource):
    """Postgres data source with optional incremental extraction and parallel chunks.

    To enable incremental mode, include in source_config:
        "incremental_column": "updated_at"   # column to track
        "watermark_table": "pipeline_config.watermarks"  # where watermark is stored

    To enable parallel extraction:
        "num_parallel_chunks": 4  # number of parallel connections
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.conn_id = config.get("conn_id", "pipeline_config_db")
        self.schema = config.get("schema", "public")
        self.table = config["table"]
        self.incremental_column = config.get("incremental_column")
        self.watermark_table = config.get("watermark_table", "pipeline_config.watermarks")
        self.num_parallel_chunks = config.get("num_parallel_chunks", 1)
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

    def _fetch_chunk(self, query: str, params: List[Any],
                     offset: int, limit: int) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Fetch a single chunk of rows using a dedicated connection."""
        hook = EtlPostgresHook(self.conn_id)
        conn = hook.get_conn()
        paginated = f"{query} LIMIT %s OFFSET %s"
        with conn.cursor() as cur:
            cur.execute(paginated, params + [limit, offset])
            columns = [desc[0] for desc in cur.description]
            chunk = [dict(zip(columns, row)) for row in cur.fetchall()]
        hook.close()
        return chunk, columns

    def extract(self, context: Optional[dict] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
        started_at = time.time()
        query, params = self._build_query()

        log.info(f"Extracting from {self.schema}.{self.table} "
                 f"{'(incremental)' if self.incremental_column else '(full)'}"
                 f"{f' ({self.num_parallel_chunks} parallel chunks)' if self.num_parallel_chunks > 1 else ''}")
        if self.incremental_column and params:
            log.info(f"Watermark: {params[0]}")

        if self.num_parallel_chunks > 1:
            total = self.get_row_count()
            if self.incremental_column and params:
                total = self.hook.get_row_count(self.schema, self.table)
            chunk_size = max(total // self.num_parallel_chunks, 1)

            all_rows = []
            columns = []
            with ThreadPoolExecutor(max_workers=self.num_parallel_chunks) as pool:
                futures = [
                    pool.submit(self._fetch_chunk, query, params, i * chunk_size, chunk_size)
                    for i in range(self.num_parallel_chunks)
                ]
                for future in as_completed(futures):
                    chunk, cols = future.result()
                    all_rows.extend(chunk)
                    if not columns:
                        columns = cols
        else:
            conn = self.hook.get_conn()
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                all_rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        if self.incremental_column and all_rows:
            max_val = max(row[self.incremental_column] for row in all_rows)
            self._set_watermark(max_val)
            log.info(f"Updated watermark to {max_val}")

        elapsed = round(time.time() - started_at, 2)
        log.info(f"Extracted {len(all_rows)} rows from {self.schema}.{self.table} in {elapsed}s")
        return all_rows, columns

    def get_row_count(self) -> int:
        return self.hook.get_row_count(self.schema, self.table)

    def close(self):
        if self._hook:
            self._hook.close()
