from unittest.mock import MagicMock, patch

from destinations.s3_dlq_destination import S3DlqDestination


class TestS3DlqDestination:
    @patch("destinations.s3_dlq_destination.EtlS3Hook")
    def test_write(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        dest = S3DlqDestination(
            {
                "conn_id": "test_s3",
                "bucket": "dlq-data",
                "prefix": "dlq/",
            }
        )

        rows = [{"id": 1, "name": None}, {"id": 2, "name": None}]
        key = dest.write(rows, pipeline_id="test_pipe", execution_date="2024-01-01")

        assert key == "dlq/test_pipe/2024-01-01/quality_failure.jsonl"
        mock_hook.get_client.return_value.upload_fileobj.assert_called_once()

    @patch("destinations.s3_dlq_destination.EtlS3Hook")
    def test_write_empty_rows(self, mock_hook_class):
        dest = S3DlqDestination(
            {
                "conn_id": "test_s3",
                "bucket": "dlq-data",
                "prefix": "dlq/",
            }
        )

        key = dest.write([], pipeline_id="test_pipe")
        assert key == ""

    @patch("destinations.s3_dlq_destination.EtlS3Hook")
    def test_write_no_date_fallback(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        dest = S3DlqDestination(
            {
                "conn_id": "test_s3",
                "bucket": "dlq-data",
                "prefix": "dlq/",
            }
        )

        rows = [{"id": 1}]
        key = dest.write(rows, pipeline_id="test_pipe")

        assert "dlq/test_pipe/" in key
        assert ".jsonl" in key

    @patch("destinations.s3_dlq_destination.EtlS3Hook")
    def test_write_failure_type_in_key(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        dest = S3DlqDestination(
            {
                "conn_id": "test_s3",
                "bucket": "dlq-data",
                "prefix": "dlq/",
            }
        )

        rows = [{"id": 1}]
        key = dest.write(
            rows,
            pipeline_id="test_pipe",
            failure_type="range: value '-5' outside [0, 100] in 'price'",
        )

        assert "range__value_" in key
        assert "outside" in key

    @patch("destinations.s3_dlq_destination.EtlS3Hook")
    def test_close(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        dest = S3DlqDestination(
            {
                "conn_id": "test_s3",
                "bucket": "dlq-data",
                "prefix": "dlq/",
            }
        )
        dest._hook = mock_hook
        dest.close()

        mock_hook.close.assert_called_once()

    @patch("destinations.s3_dlq_destination.EtlS3Hook")
    def test_close_noop(self, mock_hook_class):
        dest = S3DlqDestination(
            {
                "conn_id": "test_s3",
                "bucket": "dlq-data",
                "prefix": "dlq/",
            }
        )
        dest.close()
