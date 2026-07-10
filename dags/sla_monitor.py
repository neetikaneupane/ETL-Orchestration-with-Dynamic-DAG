"""
sla_monitor.py
==============
Monitors pipeline health and SLA compliance by querying the
task_run_metadata and pipeline_health_summary tables.
Runs hourly to detect breaches and update health aggregates.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.alerting import notify_sla_breach, notify_consecutive_failures
from utils.db import get_pg_conn

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="sla_monitor",
    description="Hourly SLA and health monitoring for all pipelines",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["monitoring", "sla", "health"],
    max_active_runs=1,
)


def check_sla_breaches(**context):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT m.pipeline_id, m.execution_date, m.duration_seconds,
                       p.sla_minutes
                FROM pipeline_config.task_run_metadata m
                JOIN pipeline_config.pipeline_definitions p
                  ON m.pipeline_id = p.pipeline_id
                WHERE m.execution_date >= NOW() - INTERVAL '24 hours'
                  AND m.status = 'success'
                  AND m.duration_seconds > (p.sla_minutes * 60)
                  AND NOT EXISTS (
                      SELECT 1 FROM pipeline_config.sla_breach_log l
                      WHERE l.pipeline_id = m.pipeline_id
                        AND l.execution_date = m.execution_date
                  )
            """)
            breaches = cur.fetchall()

            for row in breaches:
                pipeline_id, execution_date, duration_sec, sla_minutes = row
                breach_minutes = round((duration_sec / 60) - sla_minutes, 2)
                cur.execute("""
                    INSERT INTO pipeline_config.sla_breach_log
                        (pipeline_id, dag_run_id, execution_date, breach_minutes)
                    VALUES (%s, NULL, %s, %s)
                """, (pipeline_id, execution_date, breach_minutes))
                notify_sla_breach(pipeline_id, breach_minutes)
                log.warning(
                    f"SLA breach: {pipeline_id} exceeded {sla_minutes}m by {breach_minutes}m"
                )
        conn.commit()
        return len(breaches)
    finally:
        conn.close()


def update_health_summary(**context):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_config.pipeline_health_summary
                    (pipeline_id, summary_date, total_runs, successful_runs,
                     failed_runs, avg_duration_seconds, total_rows_processed)
                SELECT
                    pipeline_id,
                    DATE(execution_date) AS summary_date,
                    COUNT(*) AS total_runs,
                    COUNT(*) FILTER (WHERE status = 'success') AS successful_runs,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
                    COALESCE(AVG(duration_seconds) FILTER (WHERE status = 'success'), 0),
                    COALESCE(SUM(rows_processed), 0)
                FROM pipeline_config.task_run_metadata
                WHERE execution_date >= NOW() - INTERVAL '24 hours'
                GROUP BY pipeline_id, DATE(execution_date)
                ON CONFLICT (pipeline_id, summary_date)
                DO UPDATE SET
                    total_runs = EXCLUDED.total_runs,
                    successful_runs = EXCLUDED.successful_runs,
                    failed_runs = EXCLUDED.failed_runs,
                    avg_duration_seconds = EXCLUDED.avg_duration_seconds,
                    total_rows_processed = EXCLUDED.total_rows_processed,
                    updated_at = NOW()
            """)
            conn.commit()
            log.info("Health summary updated.")
    finally:
        conn.close()


def alert_on_consecutive_failures(**context):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pipeline_id, COUNT(*) AS consecutive_failures
                FROM (
                    SELECT pipeline_id, status, execution_date,
                           ROW_NUMBER() OVER (
                               PARTITION BY pipeline_id ORDER BY execution_date DESC
                           ) AS rn
                    FROM pipeline_config.task_run_metadata
                    WHERE execution_date >= NOW() - INTERVAL '6 hours'
                ) sub
                WHERE status = 'failed' AND rn <= 5
                GROUP BY pipeline_id
                HAVING COUNT(*) >= 3
            """)
            critical = cur.fetchall()
            for pipeline_id, failures in critical:
                notify_consecutive_failures(pipeline_id, failures)
                log.error(
                    f"ALERT: {pipeline_id} has {failures} consecutive failures in last 6h"
                )
        return len(critical)
    finally:
        conn.close()


with dag:
    t1 = PythonOperator(
        task_id="check_sla_breaches",
        python_callable=check_sla_breaches,
    )

    t2 = PythonOperator(
        task_id="update_health_summary",
        python_callable=update_health_summary,
    )

    t3 = PythonOperator(
        task_id="alert_consecutive_failures",
        python_callable=alert_on_consecutive_failures,
    )

    t1 >> t2 >> t3
