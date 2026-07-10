import pytest
from unittest.mock import MagicMock, patch


class TestEtlCallbacks:
    @patch("callbacks.etl_callbacks.get_pg_conn")
    def test_log_metadata_success(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        context = {
            "ti": MagicMock(),
            "dag": MagicMock(),
            "dag_run": MagicMock(),
            "execution_date": "2024-01-01",
        }
        context["ti"].task_id = "extract"
        context["ti"].xcom_pull.return_value = 0
        context["ti"].try_number = 1
        context["dag"].dag_id = "test_pipeline"
        context["dag_run"].run_id = "run_1"

        from callbacks.etl_callbacks import _log_metadata
        _log_metadata(context, "success")

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_on_success_callback(self):
        with patch("callbacks.etl_callbacks._log_metadata") as mock_log:
            context = {
                "ti": MagicMock(),
                "dag": MagicMock(),
                "dag_run": MagicMock(),
                "execution_date": "2024-01-01",
            }
            context["ti"].task_id = "extract"

            from callbacks.etl_callbacks import on_success_callback
            on_success_callback(context)

            mock_log.assert_called_once_with(context, "success")

    @patch("callbacks.etl_callbacks.notify_pipeline_failure")
    @patch("callbacks.etl_callbacks.get_retry_policy")
    def test_on_failure_callback_with_alert(self, mock_policy, mock_notify):
        mock_policy.return_value = {"alert_on_failure": True}

        with patch("callbacks.etl_callbacks._log_metadata"):
            context = {
                "ti": MagicMock(),
                "dag": MagicMock(),
                "dag_run": MagicMock(),
                "execution_date": "2024-01-01",
                "exception": ValueError("test error"),
            }
            context["ti"].task_id = "extract"
            context["dag"].dag_id = "test_pipeline"
            context["dag_run"].run_id = "run_1"

            from callbacks.etl_callbacks import on_failure_callback
            on_failure_callback(context)

            mock_notify.assert_called_once()

    @patch("callbacks.etl_callbacks.notify_pipeline_failure")
    @patch("callbacks.etl_callbacks.get_retry_policy")
    def test_on_failure_callback_no_alert(self, mock_policy, mock_notify):
        mock_policy.return_value = {"alert_on_failure": False}

        with patch("callbacks.etl_callbacks._log_metadata"):
            context = {
                "ti": MagicMock(),
                "dag": MagicMock(),
                "dag_run": MagicMock(),
                "execution_date": "2024-01-01",
                "exception": ValueError("test error"),
            }
            context["ti"].task_id = "extract"
            context["dag"].dag_id = "test_pipeline"
            context["dag_run"].run_id = "run_1"

            from callbacks.etl_callbacks import on_failure_callback
            on_failure_callback(context)

            mock_notify.assert_not_called()

    @patch("callbacks.etl_callbacks.compute_backoff_delay")
    @patch("callbacks.etl_callbacks.get_retry_policy")
    def test_on_retry_callback(self, mock_policy, mock_backoff):
        mock_policy.return_value = {"base_delay": 60, "max_delay": 3600}
        mock_backoff.return_value = 120

        with patch("callbacks.etl_callbacks._log_metadata"):
            context = {
                "ti": MagicMock(),
                "dag": MagicMock(),
                "dag_run": MagicMock(),
                "execution_date": "2024-01-01",
                "exception": ValueError("test error"),
            }
            context["ti"].task_id = "extract"
            context["ti"].try_number = 2
            context["dag"].dag_id = "test_pipeline"

            from callbacks.etl_callbacks import on_retry_callback
            on_retry_callback(context)

            mock_backoff.assert_called_once()

    @patch("callbacks.etl_callbacks.get_pg_conn")
    def test_sla_miss_callback(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mock_dag = MagicMock()
        mock_dag.dag_id = "test_pipeline"

        mock_sla = MagicMock()
        mock_sla.dag_id = "test_pipeline"
        mock_sla.dag_run.run_id = "run_1"
        mock_sla.execution_date = "2024-01-01"
        mock_sla.duration.total_seconds.return_value = 5400.0

        from callbacks.etl_callbacks import sla_miss_callback
        sla_miss_callback(mock_dag, [], [], [mock_sla], [])

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
