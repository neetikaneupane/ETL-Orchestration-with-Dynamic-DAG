import pytest
from unittest.mock import MagicMock, patch

from hooks.etl_postgres_hook import EtlPostgresHook


class TestEtlPostgresHook:
    @patch("hooks.etl_postgres_hook.BaseHook.get_connection")
    def test_extract_data(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "localhost"
        mock_conn_details.port = 5432
        mock_conn_details.schema = "testdb"
        mock_conn_details.login = "user"
        mock_conn_details.password = "pass"
        mock_get_conn.return_value = mock_conn_details

        hook = EtlPostgresHook("test_conn")

        with patch("hooks.etl_postgres_hook.psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.description = [("id",), ("name",)]
            mock_cursor.fetchmany.side_effect = [
                [{"id": 1, "name": "test"}],
                [],
            ]

            results = list(hook.extract_data("public", "test_table"))
            assert len(results) == 1
            rows, cols = results[0]
            assert rows == [{"id": 1, "name": "test"}]
            assert cols == ["id", "name"]

    @patch("hooks.etl_postgres_hook.BaseHook.get_connection")
    def test_get_row_count(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "localhost"
        mock_conn_details.port = 5432
        mock_conn_details.schema = "testdb"
        mock_conn_details.login = "user"
        mock_conn_details.password = "pass"
        mock_get_conn.return_value = mock_conn_details

        hook = EtlPostgresHook("test_conn")

        with patch("hooks.etl_postgres_hook.psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = (42,)

            count = hook.get_row_count("public", "test_table")
            assert count == 42
