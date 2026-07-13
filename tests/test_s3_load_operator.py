from unittest.mock import MagicMock, patch

from operators.s3_load_operator import S3LoadOperator


class TestS3LoadOperator:
    @patch("operators.s3_load_operator.get_destination")
    def test_execute_loads_data(self, mock_get_dest):
        mock_dest = MagicMock()
        mock_get_dest.return_value = mock_dest
        mock_dest.load.return_value = 1

        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
        }
        context["ti"].xcom_pull.return_value = [{"id": 1}]

        op = S3LoadOperator(
            task_id="load",
            dest_config={"bucket": "test-bucket", "prefix": "test/"},
        )

        result = op.execute(context)

        assert result["rows_loaded"] == 1
        mock_dest.load.assert_called_once()
        mock_dest.close.assert_called_once()

    @patch("operators.s3_load_operator.get_destination")
    def test_execute_no_rows(self, mock_get_dest):
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
        }
        context["ti"].xcom_pull.return_value = []

        op = S3LoadOperator(
            task_id="load",
            dest_config={"bucket": "test-bucket", "prefix": "test/"},
        )

        result = op.execute(context)
        assert result["rows_loaded"] == 0
        mock_get_dest.assert_not_called()
