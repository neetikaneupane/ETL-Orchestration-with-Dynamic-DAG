import logging
import time
from typing import Any, Dict

from airflow.models import BaseOperator
from sources import get_source

log = logging.getLogger(__name__)


class PostgresExtractOperator(BaseOperator):
    """Extracts data from a Postgres source table using the abstraction layer."""

    template_fields = ("source_config",)

    def __init__(self, source_config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.source_config = source_config
        self._source = None

    def execute(self, context):
        self._source = get_source("postgres", self.source_config)
        started_at = time.time()

        rows, columns = self._source.extract(context)

        duration = round(time.time() - started_at, 2)
        log.info(f"Extracted {len(rows)} rows in {duration}s")

        context["ti"].xcom_push(key="extracted_rows", value=rows)
        context["ti"].xcom_push(key="columns", value=columns)
        context["ti"].xcom_push(key="row_count", value=len(rows))
        context["ti"].xcom_push(key="duration_seconds", value=duration)
        context["ti"].xcom_push(key="source_config", value=self.source_config)

        self._source.close()
        return {"rows": len(rows), "duration": duration}
