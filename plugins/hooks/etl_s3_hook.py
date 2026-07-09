import json
import logging
import io
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class EtlS3Hook(BaseHook):
    """Hook for interacting with S3/MinIO destinations in the ETL pipeline."""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self._client = None

    def get_client(self):
        if self._client is None:
            conn_details = BaseHook.get_connection(self.conn_id)
            extras = conn_details.extra_dejson or {}
            endpoint_url = extras.get("endpoint_url") or conn_details.host
            self._client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=conn_details.login or extras.get("aws_access_key_id"),
                aws_secret_access_key=conn_details.password or extras.get("aws_secret_access_key"),
                region_name=extras.get("region_name", "us-east-1"),
            )
        return self._client

    def load_data(self, rows: List[Dict[str, Any]], bucket: str, key: str) -> int:
        client = self.get_client()
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            client.create_bucket(Bucket=bucket)
            log.info(f"Created bucket: {bucket}")

        data = "\n".join(json.dumps(row) for row in rows)
        body = io.BytesIO(data.encode("utf-8"))
        client.upload_fileobj(body, bucket, key)
        log.info(f"Loaded {len(rows)} rows to s3://{bucket}/{key}")
        return len(rows)

    def list_keys(self, bucket: str, prefix: str = "") -> List[str]:
        client = self.get_client()
        try:
            response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError:
            return []

    def close(self):
        self._client = None
