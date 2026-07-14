import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from hooks.etl_s3_hook import EtlS3Hook

log = logging.getLogger(__name__)


class S3DlqDestination:
    """Writes failed records to S3/MinIO as JSONL for the Dead Letter Queue."""

    def __init__(self, config: dict):
        self.conn_id = config.get("conn_id", "minio_s3")
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "dlq/")
        self._hook: Optional[EtlS3Hook] = None

    @property
    def hook(self) -> EtlS3Hook:
        if self._hook is None:
            self._hook = EtlS3Hook(self.conn_id)
        return self._hook

    def write(
        self,
        rows: List[Dict[str, Any]],
        pipeline_id: str,
        execution_date: Optional[str] = None,
        failure_type: str = "quality_failure",
    ) -> str:
        """Write failed rows to S3 DLQ location.

        Returns the S3 key where records were written.
        """
        if not rows:
            return ""

        ds = execution_date or datetime.now().strftime("%Y-%m-%d")
        safe_type = failure_type.replace(" ", "_").replace(":", "_")[:50]
        key = f"{self.prefix}{pipeline_id}/{ds}/{safe_type}.jsonl"

        data = "\n".join(json.dumps(row) for row in rows)
        import io

        body = io.BytesIO(data.encode("utf-8"))
        self.hook.get_client().upload_fileobj(body, self.bucket, key)

        log.info(f"DLQ: wrote {len(rows)} failed rows to s3://{self.bucket}/{key}")
        return key

    def close(self):
        if self._hook:
            self._hook.close()
