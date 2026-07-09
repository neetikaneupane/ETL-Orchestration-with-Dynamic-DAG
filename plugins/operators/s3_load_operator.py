import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from hooks.etl_s3_hook import EtlS3Hook

log = logging.getLogger(__name__)


class S3LoadOperator(BaseOperator):
    """Loads transformed data to S3/MinIO as JSON Lines files."""

    def __init__(self, conn_id: str, dest_config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.dest_config = dest_config

    def execute(self, context):
        ti = context["ti"]
        rows = ti.xcom_pull(task_ids="transform", key="transformed_rows")

        if not rows:
            log.info("No rows to load.")
            return {"rows_loaded": 0}

        hook = EtlS3Hook(self.conn_id)
        started_at = time.time()

        bucket = self.dest_config["bucket"]
        prefix = self.dest_config.get("prefix", "")
        ds = context["ds"]
        execution_date = context["execution_date"].strftime("%Y-%m-%dT%H:%M:%S")

        key = f"{prefix}{ds}/data_{execution_date}.jsonl"

        row_count = 0
        chunk_size = 5000
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            chunk_key = key.replace(".jsonl", f"_part{i // chunk_size:03d}.jsonl")
            count = hook.load_data(chunk, bucket, chunk_key)
            row_count += count

        duration = round(time.time() - started_at, 2)
        log.info(f"Loaded {row_count} rows to s3://{bucket}/{prefix} in {duration}s")

        ti.xcom_push(key="load_key", value=key)
        ti.xcom_push(key="load_duration", value=duration)
        ti.xcom_push(key="rows_loaded", value=row_count)

        hook.close()
        return {"rows_loaded": row_count, "key": key, "duration": duration}
