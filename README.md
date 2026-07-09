# Self-Healing ETL Pipeline

A **metadata-driven, self-healing ETL framework** built on Apache Airflow 2.8.1 that dynamically generates pipelines from database definitions and automatically handles failures, retries, SLA monitoring, and health tracking.

## Architecture

```
Postgres (Config Store)     MinIO (Data Lake)
        |                        |
        v                        v
   ┌─────────────────────────────────┐
   │   Airflow (CeleryExecutor)      │
   │  ┌─────────┐ ┌────────┐ ┌───┐  │
   │  │Scheduler│ │Worker  │ │Web│  │
   │  └─────────┘ └────────┘ └───┘  │
   └─────────────────────────────────┘
        |                        ^
        v                        |
   Redis (Broker)       Streamlit Dashboard
```

## Quick Start

```bash
# 1. Start all services
docker-compose up -d

# 2. Access Airflow UI
open http://localhost:8080   # admin / admin

# 3. Access MinIO console
open http://localhost:9001   # minioadmin / minioadmin

# 4. Launch dashboard (on host)
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

## How It Works

### 1. Dynamic DAG Generation
Pipeline definitions are stored as rows in `pipeline_config.pipeline_definitions`. The DAG factory (`dags/factory/dag_factory.py`) reads these at parse time and dynamically creates one DAG per active row. Adding/removing rows in the DB adds/removes DAGs in ~30 seconds.

### 2. ETL Pipeline Flow
Each pipeline runs: **Extract** → **Transform** → **Load**

| Step | Operator | Description |
|------|----------|-------------|
| Extract | `PostgresExtractOperator` | Reads data from a Postgres source table |
| Transform | `TransformOperator` | Applies column drops, renames, and filters |
| Load | `S3LoadOperator` | Writes transformed data as JSONL to S3/MinIO |

### 3. Self-Healing
- **Retry policies** per pipeline/error-type (exponential backoff)
- **Automatic retry logging** via `on_retry_callback`
- **SLA breach detection** via `sla_monitor.py` DAG (hourly)
- **Consecutive failure alerts** — triggers after 3+ failures in 6 hours
- **Health summaries** automatically aggregated daily

### 4. Monitoring
- **Airflow UI** — native DAG/task monitoring at http://localhost:8080
- **Streamlit Dashboard** — real-time health, SLA, and run metrics
- **SLA Monitor DAG** — hourly breach checks and health aggregation

## Project Structure

```
├── config/                  # YAML configuration files
├── dags/
│   ├── factory/
│   │   └── dag_factory.py   # Dynamic DAG generator
│   └── sla_monitor.py       # SLA & health monitoring DAG
├── plugins/
│   ├── hooks/               # Postgres & S3 connection hooks
│   ├── operators/           # ETL operators (extract, transform, load)
│   └── callbacks/           # Task lifecycle callbacks
├── dashboard/               # Streamlit monitoring dashboard
├── scripts/init/            # DB schema & seed data
├── tests/                   # pytest unit tests
├── docker-compose.yml       # Full Airflow stack
└── requirements.txt         # Python dependencies
```

## Adding a New Pipeline

Insert a row into the DB — a new DAG appears automatically:

```sql
INSERT INTO pipeline_config.pipeline_definitions
    (pipeline_id, pipeline_name, schedule_interval,
     source_type, source_config, dest_type, dest_config)
VALUES (
    'my_new_pipeline',
    'My New Pipeline',
    '0 3 * * *',
    'postgres',
    '{"conn_id": "pipeline_config_db", "schema": "public", "table": "my_table"}',
    's3',
    '{"conn_id": "minio_s3", "bucket": "processed-data", "prefix": "my_pipeline/"}'
);
```

## Running Tests

```bash
pip install -r requirements.txt
pytest
```
