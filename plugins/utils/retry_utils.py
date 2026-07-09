"""
retry_utils.py
==============
Look up retry_policies for a given pipeline and error type.
Computes exponential backoff delays and effective retry limits.
"""

import logging
from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


def _get_pg_conn():
    conn_details = BaseHook.get_connection("pipeline_config_db")
    import psycopg2
    return psycopg2.connect(
        host=conn_details.host,
        port=conn_details.port or 5432,
        dbname=conn_details.schema,
        user=conn_details.login,
        password=conn_details.password,
    )


def get_retry_policy(pipeline_id: str, error_type: str) -> Optional[Dict[str, Any]]:
    """Look up the retry policy for a pipeline + error type combo.

    Falls back to the pipeline_definitions defaults if no specific policy exists.
    """
    try:
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT retry_strategy, max_retries, base_delay_seconds,
                       max_delay_seconds, alert_on_failure
                FROM pipeline_config.retry_policies
                WHERE pipeline_id = %s AND error_type = %s
            """, (pipeline_id, error_type))
            row = cur.fetchone()
        conn.close()

        if row:
            return {
                "strategy": row[0],
                "max_retries": row[1],
                "base_delay": row[2],
                "max_delay": row[3],
                "alert_on_failure": row[4],
            }
        return None
    except Exception as e:
        log.warning(f"Failed to fetch retry policy for {pipeline_id}/{error_type}: {e}")
        return None


def compute_backoff_delay(retry_count: int, policy: Optional[Dict[str, Any]] = None,
                          base_delay: int = 60, max_delay: int = 3600) -> int:
    """Compute exponential backoff delay based on retry count.

    Default: 60s, 120s, 240s, 480s ... capped at max_delay.
    """
    if policy:
        base_delay = policy.get("base_delay", base_delay)
        max_delay = policy.get("max_delay", max_delay)

    delay = min(base_delay * (2 ** (retry_count - 1)), max_delay)
    return delay


def get_effective_max_retries(pipeline_id: str, error_type: str,
                              default_retries: int = 3) -> int:
    """Get the max retries for a pipeline + error type, falling back to default."""
    policy = get_retry_policy(pipeline_id, error_type)
    if policy:
        return policy["max_retries"]
    return default_retries
