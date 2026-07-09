"""
backfill_manager.py
===================
Periodically checks pipeline_config.backfill_requests for pending
backfill requests and replays the target pipeline for each date in
the requested range.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import DagRun, DagBag
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="backfill_manager",
    description="Process pending backfill requests and replay historical runs",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["backfill", "operations"],
    max_active_runs=1,
)


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


def process_backfill_requests(**context):
    conn = _get_pg_conn()
    dag_bag = DagBag()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, pipeline_id, gap_start_date, gap_end_date
            FROM pipeline_config.backfill_requests
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 5
        """)
        requests = cur.fetchall()

    if not requests:
        log.info("No pending backfill requests found.")
        conn.close()
        return 0

    total_triggers = 0
    for req_id, pipeline_id, start_date, end_date in requests:
        current = start_date
        dates = []
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)

        log.info(
            f"Backfill {pipeline_id} from {start_date} to {end_date} "
            f"({len(dates)} days)"
        )

        with conn.cursor() as up_cur:
            up_cur.execute(
                "UPDATE pipeline_config.backfill_requests "
                "SET status = 'running' WHERE id = %s",
                (req_id,),
            )
        conn.commit()

        triggered = 0
        failed = False
        for exec_date in dates:
            dag_id = pipeline_id
            if dag_id not in dag_bag.dags:
                log.warning(f"DAG {dag_id} not found in DagBag, skipping")
                failed = True
                continue

            logical_date = datetime.combine(exec_date, datetime.min.time())
            existing = DagRun.find(
                dag_id=dag_id,
                execution_date=logical_date,
            )
            if existing:
                log.info(f"Run already exists for {dag_id} on {exec_date}, skipping")
                triggered += 1
                continue

            try:
                dag_bag.get_dag(dag_id).create_dagrun(
                    run_id=f"backfill__{logical_date.isoformat()}",
                    logical_date=logical_date,
                    data_interval=(logical_date, logical_date + timedelta(days=1)),
                    start_date=datetime.now(),
                    state=DagRunState.QUEUED,
                    run_type=DagRunType.BACKFILL_JOB,
                    conf={"backfill_request_id": req_id},
                )
                triggered += 1
                log.info(f"Triggered backfill run for {dag_id} on {exec_date}")
            except Exception as e:
                log.error(f"Failed to trigger {dag_id} on {exec_date}: {e}")
                failed = True

        with conn.cursor() as up_cur:
            new_status = "completed" if not failed else "failed"
            up_cur.execute(
                "UPDATE pipeline_config.backfill_requests "
                "SET status = %s WHERE id = %s",
                (new_status, req_id),
            )
        conn.commit()
        total_triggers += triggered

    conn.close()
    return total_triggers


with dag:
    process = PythonOperator(
        task_id="process_backfill_requests",
        python_callable=process_backfill_requests,
    )
