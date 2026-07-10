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
        mock_conn_details.extra = None
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
        mock_conn_details.extra = None
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

    @patch("hooks.etl_postgres_hook.BaseHook.get_connection")
    def test_get_conn_reuse(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "localhost"
        mock_conn_details.port = 5432
        mock_conn_details.schema = "testdb"
        mock_conn_details.login = "user"
        mock_conn_details.password = "pass"
        mock_conn_details.extra = None
        mock_get_conn.return_value = mock_conn_details

        hook = EtlPostgresHook("test_conn")

        with patch("hooks.etl_postgres_hook.psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_conn.closed = False
            mock_connect.return_value = mock_conn

            conn1 = hook.get_conn()
            conn2 = hook.get_conn()
            assert conn1 is conn2
            assert mock_connect.call_count == 1

    @patch("hooks.etl_postgres_hook.BaseHook.get_connection")
    def test_get_conn_reconnect_when_closed(self, mock_get_conn):
        mock_conn_details = MagicMock()
        mock_conn_details.host = "localhost"
        mock_conn_details.port = 5432
        mock_conn_details.schema = "testdb"
        mock_conn_details.login = "user"
        mock_conn_details.password = "pass"
        mock_conn_details.extra = None
        mock_get_conn.return_value = mock_conn_details

        hook = EtlPostgresHook("test_conn")

        with patch("hooks.etl_postgres_hook.psycopg2.connect") as mock_connect:
            mock_conn1 = MagicMock()
            mock_conn1.closed = True
            mock_conn2 = MagicMock()
            mock_conn2.closed = False
            mock_connect.side_effect = [mock_conn1, mock_conn2]

            conn1 = hook.get_conn()
            conn2 = hook.get_conn()
            assert conn1 is not conn2
            assert mock_connect.call_count == 2

    def test_close(self):
        hook = EtlPostgresHook("test_conn")
        mock_conn = MagicMock()
        mock_conn.closed = False
        hook._conn = mock_conn

        hook.close()
        mock_conn.close.assert_called_once()

    def test_close_noop_when_not_connected(self):
        hook = EtlPostgresHook("test_conn")
        hook.close()
