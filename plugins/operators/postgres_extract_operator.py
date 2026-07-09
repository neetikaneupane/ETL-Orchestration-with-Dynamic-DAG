import logging
import time
from typing import Any, Dict, List

from airflow.models import BaseOperator
from hooks.etl_postgres_hook import EtlPostgresHook

log = logging.getLogger(__name__)


class PostgresExtractOperator(BaseOperator):
    """Extracts data from a Postgres source table and pushes to XCom."""

    template_fields = ("source_config",)

    def __init__(self, conn_id: str, source_config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.source_config = source_config

    def execute(self, context):
        schema = self.source_config.get("schema", "public")
        table = self.source_config["table"]

        hook = EtlPostgresHook(self.conn_id)
        started_at = time.time()

        total_rows = 0
        total_bytes = 0
        all_rows = []
        columns = []

        for chunk, cols in hook.extract_data(schema, table):
            all_rows.extend(chunk)
            total_rows += len(chunk)
            if not columns:
                columns = cols

        duration = round(time.time() - started_at, 2)

        log.info(f"Extracted {total_rows} rows from {schema}.{table} in {duration}s")

        context["ti"].xcom_push(key="extracted_rows", value=all_rows)
        context["ti"].xcom_push(key="columns", value=columns)
        context["ti"].xcom_push(key="row_count", value=total_rows)
        context["ti"].xcom_push(key="duration_seconds", value=duration)
        context["ti"].xcom_push(key="source_config", value=self.source_config)

        hook.close()
        return {"rows": total_rows, "duration": duration}
