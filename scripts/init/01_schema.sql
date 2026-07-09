CREATE SCHEMA IF NOT EXISTS pipeline_config;

CREATE TABLE IF NOT EXISTS pipeline_config.pipeline_definitions (
    id                  SERIAL PRIMARY KEY,
    pipeline_id         VARCHAR(100) NOT NULL UNIQUE,
    pipeline_name       VARCHAR(255) NOT NULL,
    description         TEXT,
    schedule_interval   VARCHAR(100) NOT NULL DEFAULT '@daily',
    start_date          TIMESTAMP NOT NULL DEFAULT '2024-01-01',
    catchup             BOOLEAN NOT NULL DEFAULT FALSE,
    source_type         VARCHAR(50) NOT NULL,
    source_config       JSONB NOT NULL DEFAULT '{}',
    dest_type           VARCHAR(50) NOT NULL,
    dest_config         JSONB NOT NULL DEFAULT '{}',
    transform_config    JSONB NOT NULL DEFAULT '{}',
    sla_minutes         INTEGER DEFAULT 60,
    default_retries     INTEGER NOT NULL DEFAULT 3,
    default_retry_delay_seconds INTEGER NOT NULL DEFAULT 300,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    owner               VARCHAR(100) NOT NULL DEFAULT 'data-engineering',
    tags                TEXT[] DEFAULT '{}',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_config.retry_policies (
    id                  SERIAL PRIMARY KEY,
    pipeline_id         VARCHAR(100) NOT NULL REFERENCES pipeline_config.pipeline_definitions(pipeline_id) ON DELETE CASCADE,
    error_type          VARCHAR(100) NOT NULL,
    retry_strategy      VARCHAR(50) NOT NULL DEFAULT 'exponential_backoff',
    max_retries         INTEGER NOT NULL DEFAULT 3,
    base_delay_seconds  INTEGER NOT NULL DEFAULT 60,
    max_delay_seconds   INTEGER NOT NULL DEFAULT 3600,
    alert_on_failure    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(pipeline_id, error_type)
);

CREATE TABLE IF NOT EXISTS pipeline_config.task_run_metadata (
    id                  BIGSERIAL PRIMARY KEY,
    pipeline_id         VARCHAR(100) NOT NULL,
    dag_run_id          VARCHAR(255) NOT NULL,
    task_id             VARCHAR(255) NOT NULL,
    execution_date      TIMESTAMP NOT NULL,
    rows_processed      BIGINT DEFAULT 0,
    bytes_read          BIGINT DEFAULT 0,
    duration_seconds    NUMERIC(10,2) DEFAULT 0,
    status              VARCHAR(50) NOT NULL DEFAULT 'running',
    error_type          VARCHAR(100),
    retry_count         INTEGER DEFAULT 0,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_config.pipeline_health_summary (
    id                  SERIAL PRIMARY KEY,
    pipeline_id         VARCHAR(100) NOT NULL,
    summary_date        DATE NOT NULL,
    total_runs          INTEGER DEFAULT 0,
    successful_runs     INTEGER DEFAULT 0,
    failed_runs         INTEGER DEFAULT 0,
    avg_duration_seconds NUMERIC(10,2) DEFAULT 0,
    total_rows_processed BIGINT DEFAULT 0,
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(pipeline_id, summary_date)
);

CREATE TABLE IF NOT EXISTS pipeline_config.sla_breach_log (
    id                  SERIAL PRIMARY KEY,
    pipeline_id         VARCHAR(100) NOT NULL,
    dag_run_id          VARCHAR(255) NOT NULL,
    task_id             VARCHAR(255),
    execution_date      TIMESTAMP NOT NULL,
    breach_minutes      NUMERIC(10,2),
    alert_sent          BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_config.backfill_requests (
    id                  SERIAL PRIMARY KEY,
    pipeline_id         VARCHAR(100) NOT NULL,
    gap_start_date      DATE NOT NULL,
    gap_end_date        DATE NOT NULL,
    status              VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed data
INSERT INTO pipeline_config.pipeline_definitions (
    pipeline_id, pipeline_name, schedule_interval,
    source_type, source_config, dest_type, dest_config
) VALUES (
    'orders_pg_to_s3_daily',
    'Orders: Postgres to S3 Daily',
    '0 2 * * *',
    'postgres', '{"conn_id": "pipeline_config_db", "schema": "pipeline_config", "table": "task_run_metadata"}',
    's3', '{"conn_id": "minio_s3", "bucket": "processed-data", "prefix": "orders/daily/"}'
) ON CONFLICT (pipeline_id) DO NOTHING;

-- Seed backfill request (for testing the backfill DAG)
INSERT INTO pipeline_config.backfill_requests
    (pipeline_id, gap_start_date, gap_end_date, status)
VALUES (
    'orders_pg_to_s3_daily',
    '2024-06-01',
    '2024-06-05',
    'pending'
) ON CONFLICT DO NOTHING;