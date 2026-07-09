import logging
import time
from typing import Any, Dict

from airflow.models import BaseOperator
from destinations import get_destination

log = logging.getLogger(__name__)


class S3LoadOperator(BaseOperator):
    """Loads transformed data to S3/MinIO using the abstraction layer."""

    def __init__(self, dest_config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.dest_config = dest_config
        self._dest = None

    def execute(self, context):
        ti = context["ti"]
        rows = ti.xcom_pull(task_ids="transform", key="transformed_rows")

        if not rows:
            log.info("No rows to load.")
            return {"rows_loaded": 0}

        self._dest = get_destination("s3", self.dest_config)
        started_at = time.time()

        row_count = self._dest.load(rows, context)

        duration = round(time.time() - started_at, 2)
        log.info(f"Loaded {row_count} rows in {duration}s")

        ti.xcom_push(key="load_duration", value=duration)
        ti.xcom_push(key="rows_loaded", value=row_count)

        self._dest.close()
        return {"rows_loaded": row_count, "duration": duration}
