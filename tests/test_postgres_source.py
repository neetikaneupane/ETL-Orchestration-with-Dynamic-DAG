from unittest.mock import MagicMock, patch
from datetime import datetime

import psycopg2.sql as sql
from sources.postgres_source import PostgresSource


class TestPostgresSource:
    @patch("sources.postgres_source.EtlPostgresHook")
    def test_extract_full(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_conn = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "test")]

        source = PostgresSource({
            "conn_id": "test_conn",
            "schema": "public",
            "table": "test_table",
        })

        rows, columns = source.extract()

        assert len(rows) == 1
        assert columns == ["id", "name"]
        call_args = mock_cursor.execute.call_args
        assert isinstance(call_args[0][0], sql.Composed)
        assert call_args[0][1] == []

    @patch.object(PostgresSource, "_get_watermark", return_value=None)
    @patch.object(PostgresSource, "_set_watermark")
    @patch("sources.postgres_source.EtlPostgresHook")
    def test_extract_incremental(self, mock_hook_class, mock_set_wm, mock_get_wm):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_conn = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.description = [("id",), ("updated_at",)]
        mock_cursor.fetchall.return_value = [
            (1, datetime(2024, 6, 1, 10, 0, 0)),
            (2, datetime(2024, 6, 1, 11, 0, 0)),
        ]

        source = PostgresSource({
            "conn_id": "test_conn",
            "schema": "public",
            "table": "test_table",
            "incremental_column": "updated_at",
            "watermark_table": "pipeline_config.watermarks",
        })

        rows, columns = source.extract()

        assert len(rows) == 2
        assert columns == ["id", "updated_at"]
        assert rows[0]["updated_at"] == datetime(2024, 6, 1, 10, 0, 0)
        mock_set_wm.assert_called_once()
        mock_get_wm.assert_called_once()

    @patch("sources.postgres_source.EtlPostgresHook")
    def test_close(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        source = PostgresSource({
            "conn_id": "test_conn",
            "schema": "public",
            "table": "test_table",
        })
        source._hook = mock_hook
        source.close()

        mock_hook.close.assert_called_once()

    @patch("sources.postgres_source.EtlPostgresHook")
    def test_close_noop(self, mock_hook_class):
        source = PostgresSource({
            "conn_id": "test_conn",
            "schema": "public",
            "table": "test_table",
        })
        source.close()

    @patch("sources.postgres_source.EtlPostgresHook")
    def test_get_row_count(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_row_count.return_value = 42

        source = PostgresSource({
            "conn_id": "test_conn",
            "schema": "public",
            "table": "test_table",
        })

        count = source.get_row_count()
        assert count == 42
        mock_hook.get_row_count.assert_called_once_with("public", "test_table")
