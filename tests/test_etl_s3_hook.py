from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from hooks.etl_s3_hook import EtlS3Hook


class TestEtlS3Hook:
    @patch("hooks.etl_s3_hook.BaseHook.get_connection")
    def test_load_data(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "http://minio:9000"
        mock_conn_details.login = "admin"
        mock_conn_details.password = "admin123"
        mock_conn_details.extra_dejson = {}
        mock_get_conn.return_value = mock_conn_details

        hook = EtlS3Hook("test_s3")

        with patch("hooks.etl_s3_hook.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
            mock_client.head_bucket.side_effect = ClientError(
                error_response, "HeadBucket"
            )

            rows = [{"id": 1, "name": "test"}]
            count = hook.load_data(rows, "test-bucket", "test/key.jsonl")

            assert count == 1
            mock_client.create_bucket.assert_called_once_with(Bucket="test-bucket")
            mock_client.upload_fileobj.assert_called_once()

    @patch("hooks.etl_s3_hook.BaseHook.get_connection")
    def test_list_keys(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "http://minio:9000"
        mock_conn_details.login = "admin"
        mock_conn_details.password = "admin123"
        mock_conn_details.extra_dejson = {}
        mock_get_conn.return_value = mock_conn_details

        hook = EtlS3Hook("test_s3")

        with patch("hooks.etl_s3_hook.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.list_objects_v2.return_value = {
                "Contents": [{"Key": "test/file1.jsonl"}, {"Key": "test/file2.jsonl"}]
            }

            keys = hook.list_keys("test-bucket", "test/")
            assert len(keys) == 2
            assert "test/file1.jsonl" in keys

    @patch("hooks.etl_s3_hook.BaseHook.get_connection")
    def test_list_keys_client_error(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "http://minio:9000"
        mock_conn_details.login = "admin"
        mock_conn_details.password = "admin123"
        mock_conn_details.extra_dejson = {}
        mock_get_conn.return_value = mock_conn_details

        hook = EtlS3Hook("test_s3")

        with patch("hooks.etl_s3_hook.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.list_objects_v2.side_effect = ClientError(
                {"Error": {"Code": "NoSuchBucket"}}, "ListObjectsV2"
            )

            keys = hook.list_keys("nonexistent-bucket", "test/")
            assert keys == []

    @patch("hooks.etl_s3_hook.BaseHook.get_connection")
    def test_list_keys_empty(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "http://minio:9000"
        mock_conn_details.login = "admin"
        mock_conn_details.password = "admin123"
        mock_conn_details.extra_dejson = {}
        mock_get_conn.return_value = mock_conn_details

        hook = EtlS3Hook("test_s3")

        with patch("hooks.etl_s3_hook.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.list_objects_v2.return_value = {}

            keys = hook.list_keys("test-bucket", "test/")
            assert keys == []

    def test_close(self):
        hook = EtlS3Hook("test_s3")
        hook._client = MagicMock()
        hook.close()
        assert hook._client is None
