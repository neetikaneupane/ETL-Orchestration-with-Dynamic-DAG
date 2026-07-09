import pytest
from unittest.mock import MagicMock, patch

from operators.s3_load_operator import S3LoadOperator


class TestS3LoadOperator:
    @patch("operators.s3_load_operator.EtlS3Hook")
    def test_execute_loads_data(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "execution_date": MagicMock(),
        }
        context["execution_date"].strftime.return_value = "2024-01-01T00:00:00"
        context["ti"].xcom_pull.return_value = [{"id": 1}]
        mock_hook.load_data.return_value = 1

        op = S3LoadOperator(
            task_id="load",
            conn_id="test_s3",
            dest_config={"bucket": "test-bucket", "prefix": "test/"},
        )

        result = op.execute(context)

        assert result["rows_loaded"] == 1
        mock_hook.load_data.assert_called_once()

    def test_execute_no_rows(self):
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "execution_date": MagicMock(),
        }
        context["ti"].xcom_pull.return_value = []

        op = S3LoadOperator(
            task_id="load",
            conn_id="test_s3",
            dest_config={"bucket": "test-bucket", "prefix": "test/"},
        )

        result = op.execute(context)
        assert result["rows_loaded"] == 0
