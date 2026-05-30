"""
dag_factory.py
==============
Reads pipeline_definitions from Postgres at parse time and generates
one Airflow DAG per active row. 

Adding a row to the DB = new DAG appears in Airflow within 30 seconds.
Deactivating a row (is_active=False) = DAG disappears.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

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

    except Exception as e:
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

    dag = DAG(
        dag_id=config["pipeline_id"],
        description=config["pipeline_name"],
        schedule=config["schedule_interval"],
        start_date=config["start_date"],
        catchup=config["catchup"],
        default_args=default_args,
        tags=config["tags"] or [],
        max_active_runs=1,  # prevent overlapping runs
    )

    # -----------------------------------------------------------------
    # Tasks (placeholders for now — Week 2 Part 2 we make these real)
    # -----------------------------------------------------------------
    with dag:

        def extract(**context):
            log.info(f"[{config['pipeline_id']}] EXTRACT from {config['source_type']}")
            log.info(f"Source config: {config['source_config']}")
            # Push source config to XCom so next task can read it
            context["ti"].xcom_push(key="source_config", value=config["source_config"])
            return "extract_done"

        def transform(**context):
            log.info(f"[{config['pipeline_id']}] TRANSFORM")
            log.info(f"Transform config: {config['transform_config']}")
            return "transform_done"

        def load(**context):
            log.info(f"[{config['pipeline_id']}] LOAD to {config['dest_type']}")
            log.info(f"Dest config: {config['dest_config']}")
            return "load_done"

        t_extract = PythonOperator(
            task_id="extract",
            python_callable=extract,
        )

        t_transform = PythonOperator(
            task_id="transform",
            python_callable=transform,
        )

        t_load = PythonOperator(
            task_id="load",
            python_callable=load,
        )

        # Define task order: extract → transform → load
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