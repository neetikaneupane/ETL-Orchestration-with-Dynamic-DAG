import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from hooks.etl_s3_hook import EtlS3Hook
from .base_destination import BaseDestination

log = logging.getLogger(__name__)


class S3Destination(BaseDestination):
    """S3/MinIO destination that writes JSON Lines files."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.conn_id = config.get("conn_id", "minio_s3")
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "")
        self._hook: Optional[EtlS3Hook] = None

    @property
    def hook(self) -> EtlS3Hook:
        if self._hook is None:
            self._hook = EtlS3Hook(self.conn_id)
        return self._hook

    def load(self, rows: List[Dict[str, Any]], context: Optional[dict] = None) -> int:
        if not rows:
            log.info("No rows to load.")
            return 0

        ds = (
            context.get("ds", datetime.now().strftime("%Y-%m-%d"))
            if context
            else datetime.now().strftime("%Y-%m-%d")
        )
        key_prefix = f"{self.prefix}{ds}/data"

        row_count = 0
        chunk_size = 5000
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            key = f"{key_prefix}_part{i // chunk_size:03d}.jsonl"
            count = self.hook.load_data(chunk, self.bucket, key)
            row_count += count

        log.info(f"Loaded {row_count} rows to s3://{self.bucket}/{self.prefix}")
        return row_count

    def close(self):
        if self._hook:
            self._hook.close()
