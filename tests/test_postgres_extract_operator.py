import pytest
from unittest.mock import MagicMock, patch

from operators.postgres_extract_operator import PostgresExtractOperator


class TestPostgresExtractOperator:
    @patch("operators.postgres_extract_operator.get_source")
    def test_execute_extracts_data(self, mock_get_source):
        mock_source = MagicMock()
        mock_get_source.return_value = mock_source
        mock_source.extract.return_value = ([{"id": 1, "name": "test"}], ["id", "name"])

        context = {"ti": MagicMock()}
        op = PostgresExtractOperator(
            task_id="extract",
            source_config={
                "conn_id": "test_conn",
                "schema": "public",
                "table": "test_table",
            },
        )

        result = op.execute(context)

        mock_source.extract.assert_called_once()
        assert result["rows"] == 1
        mock_source.close.assert_called_once()

    @patch("operators.postgres_extract_operator.get_source")
    def test_execute_no_rows(self, mock_get_source):
        mock_source = MagicMock()
        mock_get_source.return_value = mock_source
        mock_source.extract.return_value = ([], [])

        op = PostgresExtractOperator(
            task_id="extract",
            source_config={
                "conn_id": "test_conn",
                "schema": "public",
                "table": "empty_table",
            },
        )

        context = {"ti": MagicMock()}
        result = op.execute(context)

        assert result["rows"] == 0
        mock_source.close.assert_called_once()
