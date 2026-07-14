"""
dead_letter_queue.py
====================
Dead Letter Queue (DLQ) utilities for routing failed records
out of the main ETL pipeline for later inspection and reprocessing.
"""

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


def identify_failing_rows(
    rows: List[Dict[str, Any]],
    checks: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Identify individual rows that fail quality checks.

    Returns a dict mapping failure descriptions to lists of failing row dicts.
    A single row may appear in multiple failure groups.
    """
    failing: Dict[str, List[Dict[str, Any]]] = {}

    for check in checks:
        ctype = check.get("type")

        if ctype == "not_null":
            columns = check.get("columns", [])
            for row in rows:
                for col in columns:
                    if row.get(col) is None:
                        key = f"not_null: column '{col}' is null"
                        failing.setdefault(key, []).append(row)

        elif ctype == "unique":
            column = check.get("column", "")
            seen: Dict[Any, int] = {}
            for row in rows:
                val = row.get(column)
                seen[val] = seen.get(val, 0) + 1
            for row in rows:
                val = row.get(column)
                if seen[val] > 1:
                    key = f"unique: duplicate value '{val}' in column '{column}'"
                    failing.setdefault(key, []).append(row)

        elif ctype == "range":
            column = check.get("column", "")
            min_val = check.get("min")
            max_val = check.get("max")
            for row in rows:
                val = row.get(column)
                if val is None:
                    continue
                try:
                    fval = float(val)
                    out_of_range = False
                    if min_val is not None and fval < min_val:
                        out_of_range = True
                    if max_val is not None and fval > max_val:
                        out_of_range = True
                    if out_of_range:
                        key = f"range: value '{val}' outside [{min_val}, {max_val}] in '{column}'"
                        failing.setdefault(key, []).append(row)
                except (TypeError, ValueError):
                    key = f"range: non-numeric value '{val}' in '{column}'"
                    failing.setdefault(key, []).append(row)

    return failing


def get_valid_rows(
    rows: List[Dict[str, Any]],
    checks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return rows that pass all quality checks."""
    failing = identify_failing_rows(rows, checks)
    failing_ids = set()
    for row_list in failing.values():
        for row in row_list:
            failing_ids.add(id(row))
    return [row for row in rows if id(row) not in failing_ids]


def write_dlq_metadata(
    pipeline_id: str,
    execution_date,
    dag_run_id: Optional[str],
    failure_type: str,
    failure_message: str,
    row_count: int,
    s3_key: Optional[str] = None,
    s3_bucket: Optional[str] = None,
    conn_id: str = "pipeline_config_db",
) -> None:
    """Write a DLQ entry to the dead_letter_queue table."""
    from utils.db import get_pg_conn

    conn = get_pg_conn(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_config.dead_letter_queue
                    (pipeline_id, execution_date, dag_run_id, task_id,
                     failure_type, failure_message, row_count, s3_key, s3_bucket)
                VALUES (%s, %s, %s, 'transform', %s, %s, %s, %s, %s)
                """,
                (
                    pipeline_id,
                    execution_date,
                    dag_run_id,
                    failure_type,
                    failure_message,
                    row_count,
                    s3_key,
                    s3_bucket,
                ),
            )
        conn.commit()
        log.info(
            f"DLQ metadata written: pipeline={pipeline_id}, "
            f"failure={failure_type}, rows={row_count}"
        )
    except Exception:
        conn.rollback()
        log.exception("Failed to write DLQ metadata")
        raise
    finally:
        conn.close()


def get_dlq_summary(
    pipeline_id: Optional[str] = None,
    hours: int = 24,
    conn_id: str = "pipeline_config_db",
) -> List[Dict[str, Any]]:
    """Get a summary of DLQ entries for monitoring.

    Returns a list of dicts with pipeline_id, failure_type, total_rows, entry_count.
    """
    from utils.db import get_pg_conn

    conn = get_pg_conn(conn_id)
    try:
        with conn.cursor() as cur:
            query = """
                SELECT pipeline_id, failure_type,
                       SUM(row_count) AS total_rows,
                       COUNT(*) AS entry_count
                FROM pipeline_config.dead_letter_queue
                WHERE created_at >= NOW() - INTERVAL '%s hours'
            """
            params: list = [hours]
            if pipeline_id:
                query += " AND pipeline_id = %s"
                params.append(pipeline_id)
            query += " GROUP BY pipeline_id, failure_type ORDER BY total_rows DESC"
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()
