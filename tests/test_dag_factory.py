from datetime import datetime

from dags.factory.dag_factory import build_dag


class TestDagFactory:
    def test_build_dag_creates_dag_with_correct_id(self):
        config = {
            "pipeline_id": "test_pipeline",
            "pipeline_name": "Test Pipeline",
            "schedule_interval": "@daily",
            "start_date": datetime(2024, 1, 1),
            "catchup": False,
            "source_type": "postgres",
            "source_config": {
                "conn_id": "pipeline_config_db",
                "schema": "public",
                "table": "source_table",
            },
            "dest_type": "s3",
            "dest_config": {
                "conn_id": "minio_s3",
                "bucket": "test-bucket",
                "prefix": "test/",
            },
            "transform_config": {},
            "sla_minutes": 60,
            "default_retries": 3,
            "default_retry_delay_seconds": 300,
            "owner": "test-owner",
            "tags": ["test"],
        }

        dag = build_dag(config)

        assert dag.dag_id == "test_pipeline"
        assert dag.description == "Test Pipeline"
        assert dag.schedule_interval == "@daily"
        assert dag.max_active_runs == 1

    def test_build_dag_task_order(self):
        config = {
            "pipeline_id": "order_test",
            "pipeline_name": "Order Test",
            "schedule_interval": "@daily",
            "start_date": datetime(2024, 1, 1),
            "catchup": False,
            "source_type": "postgres",
            "source_config": {
                "conn_id": "pipeline_config_db",
                "schema": "public",
                "table": "t",
            },
            "dest_type": "s3",
            "dest_config": {
                "conn_id": "minio_s3",
                "bucket": "b",
                "prefix": "p/",
            },
            "transform_config": {},
            "sla_minutes": 30,
            "default_retries": 2,
            "default_retry_delay_seconds": 60,
            "owner": "owner",
            "tags": [],
        }

        dag = build_dag(config)

        tasks = dag.tasks
        task_ids = [t.task_id for t in tasks]
        assert task_ids == ["extract", "transform", "load"]

        extract_idx = task_ids.index("extract")
        transform_idx = task_ids.index("transform")
        load_idx = task_ids.index("load")
        assert extract_idx < transform_idx < load_idx
