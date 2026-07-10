import logging
from typing import Any, Dict

from utils.db import get_pg_conn
from utils.retry_utils import get_retry_policy, compute_backoff_delay
from utils.alerting import notify_pipeline_failure, notify_sla_breach

log = logging.getLogger(__name__)


def _log_metadata(context: Dict[str, Any], status: str):
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    execution_date = context["execution_date"]
    task_id = ti.task_id

    rows = ti.xcom_pull(task_ids="extract", key="row_count") or 0
    duration = (
        ti.xcom_pull(task_ids=task_id, key="duration_seconds")
        or ti.xcom_pull(task_ids=task_id, key="transform_duration")
        or ti.xcom_pull(task_ids=task_id, key="load_duration")
        or 0
    )
    error_type = context.get("exception", None)
    if error_type is not None:
        error_type = type(error_type).__name__

    retry_count = ti.try_number - 1

    try:
        conn = get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_config.task_run_metadata
                        (pipeline_id, dag_run_id, task_id, execution_date,
                         rows_processed, duration_seconds, status, error_type, retry_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    dag_id, run_id, task_id, execution_date,
                    rows, duration, status, error_type, retry_count,
                ))
                conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Failed to log task metadata: {e}")


def on_success_callback(context: Dict[str, Any]):
    _log_metadata(context, "success")
    log.info(f"Task {context['ti'].task_id} succeeded.")


def on_failure_callback(context: Dict[str, Any]):
    _log_metadata(context, "failed")
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    error_type = context.get("exception", None)
    error_name = type(error_type).__name__ if error_type else "Unknown"
    policy = get_retry_policy(dag_id, error_name)
    run_url = context.get("dag_run", None)
    run_url = run_url.run_id if run_url else None
    if policy and policy.get("alert_on_failure"):
        notify_pipeline_failure(dag_id, ti.task_id, error_name, run_url)
    log.error(f"Task {ti.task_id} FAILED with {error_name}.")


def on_retry_callback(context: Dict[str, Any]):
    _log_metadata(context, "retrying")
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    retry_count = ti.try_number - 1
    error_type = context.get("exception", None)
    error_name = type(error_type).__name__ if error_type else "Unknown"
    policy = get_retry_policy(dag_id, error_name)
    delay = compute_backoff_delay(retry_count, policy)
    log.warning(
        f"Retry #{retry_count} for {ti.task_id} in {dag_id} "
        f"(error: {error_name}, backoff: {delay}s)"
    )


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    log.warning(f"SLA miss for DAG {dag.dag_id}")
    try:
        conn = get_pg_conn()
        try:
            with conn.cursor() as cur:
                for sla in slas:
                    cur.execute("""
                        INSERT INTO pipeline_config.sla_breach_log
                            (pipeline_id, dag_run_id, execution_date, breach_minutes)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        sla.dag_id,
                        sla.dag_run.run_id if sla.dag_run else None,
                        sla.execution_date,
                        sla.duration.total_seconds() / 60 if sla.duration else None,
                    ))
                conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Failed to log SLA breach: {e}")
