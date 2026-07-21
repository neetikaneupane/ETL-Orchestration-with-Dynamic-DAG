"""
retry_utils.py
==============
Look up retry_policies for a given pipeline and error type.
Computes backoff delays with jitter and supports multiple strategies.
"""

import logging
import random
from typing import Any, Dict, Optional

from utils.db import get_pg_conn

log = logging.getLogger(__name__)


def get_retry_policy(pipeline_id: str, error_type: str) -> Optional[Dict[str, Any]]:
    """Look up the retry policy for a pipeline + error type combo.

    Falls back to the pipeline_definitions defaults if no specific policy exists.
    """
    try:
        conn = get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT retry_strategy, max_retries, base_delay_seconds,
                           max_delay_seconds, alert_on_failure
                    FROM pipeline_config.retry_policies
                    WHERE pipeline_id = %s AND error_type = %s
                """,
                    (pipeline_id, error_type),
                )
                row = cur.fetchone()

            if row:
                return {
                    "strategy": row[0],
                    "max_retries": row[1],
                    "base_delay": row[2],
                    "max_delay": row[3],
                    "alert_on_failure": row[4],
                }
            return None
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"Failed to fetch retry policy for {pipeline_id}/{error_type}: {e}")
        return None


def compute_backoff_delay(
    retry_count: int,
    policy: Optional[Dict[str, Any]] = None,
    base_delay: int = 60,
    max_delay: int = 3600,
    apply_jitter: bool = True,
) -> int:
    """Compute backoff delay based on retry count and strategy.

    Supports 'exponential' (default), 'linear', and 'fixed' strategies.
    Applies full jitter to prevent thundering herd unless apply_jitter=False.
    """
    if policy:
        base_delay = policy.get("base_delay", base_delay)
        max_delay = policy.get("max_delay", max_delay)

    strategy = policy.get("strategy", "exponential") if policy else "exponential"

    if strategy == "linear":
        delay = min(base_delay * retry_count, max_delay)
    elif strategy == "fixed":
        delay = base_delay
    else:
        delay = min(base_delay * (2 ** (retry_count - 1)), max_delay)

    if apply_jitter:
        delay = random.uniform(0, delay)

    return max(int(delay), 1)


def get_effective_max_retries(
    pipeline_id: str, error_type: str, default_retries: int = 3
) -> int:
    """Get the max retries for a pipeline + error type, falling back to default."""
    policy = get_retry_policy(pipeline_id, error_type)
    if policy:
        return policy["max_retries"]
    return default_retries
