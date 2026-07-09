import pytest
from unittest.mock import MagicMock, patch

from destinations.s3_destination import S3Destination


class TestS3Destination:
    @patch("destinations.s3_destination.EtlS3Hook")
    def test_load(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.load_data.return_value = 2

        dest = S3Destination({
            "conn_id": "test_s3",
            "bucket": "test-bucket",
            "prefix": "test/",
        })

        rows = [{"id": 1}, {"id": 2}]
        count = dest.load(rows, {"ds": "2024-01-01"})

        assert count == 2
        mock_hook.load_data.assert_called()

    @patch("destinations.s3_destination.EtlS3Hook")
    def test_load_no_rows(self, mock_hook_class):
        dest = S3Destination({
            "conn_id": "test_s3",
            "bucket": "test-bucket",
            "prefix": "test/",
        })

        count = dest.load([])
        assert count == 0
