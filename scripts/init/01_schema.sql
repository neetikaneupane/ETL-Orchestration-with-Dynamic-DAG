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
    dlq_config          JSONB NOT NULL DEFAULT '{}',
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

CREATE TABLE IF NOT EXISTS pipeline_config.watermarks (
    id                  SERIAL PRIMARY KEY,
    source_table        VARCHAR(255) NOT NULL UNIQUE,
    watermark_value     TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_config.dead_letter_queue (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_id     VARCHAR(100) NOT NULL,
    execution_date  TIMESTAMP NOT NULL,
    dag_run_id      VARCHAR(255),
    task_id         VARCHAR(255) NOT NULL DEFAULT 'transform',
    failure_type    VARCHAR(100) NOT NULL,
    failure_message TEXT,
    row_count       INTEGER NOT NULL DEFAULT 0,
    s3_key          TEXT,
    s3_bucket       VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_date
    ON pipeline_config.dead_letter_queue (pipeline_id, execution_date);

-- Sample orders table (real source data for the demo pipeline)
CREATE TABLE IF NOT EXISTS pipeline_config.sample_orders (
    id              SERIAL PRIMARY KEY,
    order_id        VARCHAR(50) NOT NULL UNIQUE,
    customer_id     VARCHAR(50) NOT NULL,
    product         VARCHAR(255) NOT NULL,
    quantity        INTEGER NOT NULL DEFAULT 1,
    unit_price      NUMERIC(10,2) NOT NULL,
    total_amount    NUMERIC(10,2) NOT NULL,
    status          VARCHAR(50) NOT NULL DEFAULT 'pending',
    order_date      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed sample orders
INSERT INTO pipeline_config.sample_orders
    (order_id, customer_id, product, quantity, unit_price, total_amount, status, order_date)
VALUES
    ('ORD-001', 'CUST-1001', 'Widget A', 5, 19.99, 99.95, 'shipped',   '2024-06-01 08:30:00'),
    ('ORD-002', 'CUST-1002', 'Widget B', 2, 49.99, 99.98, 'delivered', '2024-06-01 09:15:00'),
    ('ORD-003', 'CUST-1001', 'Widget A', 1, 19.99, 19.99, 'pending',   '2024-06-02 10:00:00'),
    ('ORD-004', 'CUST-1003', 'Widget C', 10, 9.99, 99.90, 'shipped',   '2024-06-02 11:45:00'),
    ('ORD-005', 'CUST-1002', 'Widget B', 3, 49.99, 149.97, 'cancelled', '2024-06-03 07:20:00')
ON CONFLICT (order_id) DO NOTHING;

-- Pipeline definition pointing to sample_orders
INSERT INTO pipeline_config.pipeline_definitions (
    pipeline_id, pipeline_name, schedule_interval,
    source_type, source_config, dest_type, dest_config
) VALUES (
    'orders_pg_to_s3_daily',
    'Orders: Postgres to S3 Daily',
    '0 2 * * *',
    'postgres', '{"conn_id": "pipeline_config_db", "schema": "pipeline_config", "table": "sample_orders", "incremental_column": "updated_at"}',
    's3', '{"conn_id": "minio_s3", "bucket": "processed-data", "prefix": "orders/daily/"}'
) ON CONFLICT (pipeline_id) DO NOTHING;

-- Seed retry policies
INSERT INTO pipeline_config.retry_policies
    (pipeline_id, error_type, retry_strategy, max_retries, base_delay_seconds, max_delay_seconds, alert_on_failure)
VALUES
    ('orders_pg_to_s3_daily', 'OperationalError', 'exponential_backoff', 5, 30, 3600, TRUE),
    ('orders_pg_to_s3_daily', 'InterfaceError',   'exponential_backoff', 3, 60, 1800, TRUE),
    ('orders_pg_to_s3_daily', 'ClientError',      'exponential_backoff', 2, 120, 600, FALSE)
ON CONFLICT (pipeline_id, error_type) DO NOTHING;

-- Seed backfill request (for testing the backfill DAG)
INSERT INTO pipeline_config.backfill_requests
    (pipeline_id, gap_start_date, gap_end_date, status)
VALUES (
    'orders_pg_to_s3_daily',
    '2024-06-01',
    '2024-06-05',
    'pending'
) ON CONFLICT DO NOTHING;