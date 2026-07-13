"""
dag_factory.py
==============
Reads pipeline_definitions from Postgres at parse time and generates
one Airflow DAG per active row.

Adding a row to the DB = new DAG appears in Airflow within 30 seconds.
Deactivating a row (is_active=False) = DAG disappears.
"""

import logging
from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook

from operators import (
    PostgresExtractOperator, TransformOperator, S3LoadOperator,
)
from callbacks import on_success_callback, on_failure_callback, on_retry_callback
from utils.retry_utils import get_retry_policy, compute_backoff_delay

log = logging.getLogger(__name__)

# =============================================================================
# STEP 1: Fetch pipeline configs from Postgres
# =============================================================================
# IMPORTANT: We wrap this in a try/except because the scheduler parses this
# file before Postgres might be ready. If the fetch fails, we return an empty
# list — no DAGs are created, but Airflow doesn't crash.
# =============================================================================

def fetch_pipeline_configs():
    """
    Fetches all active pipeline definitions from the config store.
    Returns a list of dicts, one per pipeline.
    """
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        # Get connection details from Airflow's connection store
        conn_details = BaseHook.get_connection("pipeline_config_db")

        conn = psycopg2.connect(
            host=conn_details.host,
            port=conn_details.port or 5432,
            dbname=conn_details.schema,
            user=conn_details.login,
            password=conn_details.password,
            connect_timeout=5,  # never hang the scheduler
        )

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    pipeline_id,
                    pipeline_name,
                    schedule_interval,
                    start_date,
                    catchup,
                    source_type,
                    source_config,
                    dest_type,
                    dest_config,
                    transform_config,
                    sla_minutes,
                    default_retries,
                    default_retry_delay_seconds,
                    owner,
                    tags
                FROM pipeline_config.pipeline_definitions
                WHERE is_active = TRUE
                ORDER BY pipeline_id
            """)
            rows = cur.fetchall()

        conn.close()
        log.info(f"DAG factory fetched {len(rows)} active pipelines.")
        return [dict(row) for row in rows]

    except (psycopg2.OperationalError, psycopg2.InterfaceError, AirflowNotFoundException) as e:
        log.error(f"DAG factory failed to fetch configs: {e}")
        return []


# =============================================================================
# STEP 2: Build one DAG from one config row
# =============================================================================

def build_dag(config: dict) -> DAG:
    """
    Takes one row from pipeline_definitions and returns a fully 
    configured Airflow DAG object.
    """

    default_args = {
        "owner": config["owner"],
        "retries": config["default_retries"],
        "retry_delay": timedelta(seconds=config["default_retry_delay_seconds"]),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    }

    config["source_type"]
    source_config = config["source_config"]
    config["dest_type"]
    dest_config = config["dest_config"]
    transform_config = config["transform_config"]

    default_args.setdefault("on_success_callback", on_success_callback)
    default_args.setdefault("on_failure_callback", on_failure_callback)
    default_args.setdefault("on_retry_callback", on_retry_callback)

    dag = DAG(
        dag_id=config["pipeline_id"],
        description=config["pipeline_name"],
        schedule=config["schedule_interval"],
        start_date=config["start_date"],
        catchup=config["catchup"],
        default_args=default_args,
        tags=config["tags"] or [],
        max_active_runs=1,
    )

    pipeline_id = config["pipeline_id"]

    with dag:
        extract_policy = get_retry_policy(pipeline_id, "OperationalError")
        extract_retries = extract_policy["max_retries"] if extract_policy else config["default_retries"]
        extract_delay = compute_backoff_delay(1, extract_policy) if extract_policy else config["default_retry_delay_seconds"]

        t_extract = PostgresExtractOperator(
            task_id="extract",
            source_config=source_config,
            retries=extract_retries,
            retry_delay=timedelta(seconds=extract_delay),
        )

        transform_policy = get_retry_policy(pipeline_id, "ValueError")
        transform_retries = transform_policy["max_retries"] if transform_policy else config["default_retries"]
        transform_delay = compute_backoff_delay(1, transform_policy) if transform_policy else config["default_retry_delay_seconds"]

        t_transform = TransformOperator(
            task_id="transform",
            transform_config=transform_config,
            retries=transform_retries,
            retry_delay=timedelta(seconds=transform_delay),
        )

        load_policy = get_retry_policy(pipeline_id, "ClientError")
        load_retries = load_policy["max_retries"] if load_policy else config["default_retries"]
        load_delay = compute_backoff_delay(1, load_policy) if load_policy else config["default_retry_delay_seconds"]

        t_load = S3LoadOperator(
            task_id="load",
            dest_config=dest_config,
            retries=load_retries,
            retry_delay=timedelta(seconds=load_delay),
        )

        t_extract >> t_transform >> t_load

    return dag


# =============================================================================
# STEP 3: Register all DAGs into Airflow's global namespace
# =============================================================================
# Airflow discovers DAGs by scanning the global namespace of every file
# in the dags/ folder. Any variable of type DAG gets picked up.
# We name them dynamically using the pipeline_id as the variable name.
# =============================================================================

_configs = fetch_pipeline_configs()

for _config in _configs:
    dag_id = _config["pipeline_id"]
    globals()[dag_id] = build_dag(_config)
    log.info(f"DAG factory registered DAG: {dag_id}")