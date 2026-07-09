import pytest
from unittest.mock import MagicMock, patch

from operators.postgres_extract_operator import PostgresExtractOperator


class TestPostgresExtractOperator:
    @patch("operators.postgres_extract_operator.EtlPostgresHook")
    def test_execute_extracts_data(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.extract_data.return_value = [
            ([{"id": 1, "name": "test"}], ["id", "name"])
        ]

        context = {"ti": MagicMock()}
        op = PostgresExtractOperator(
            task_id="extract",
            conn_id="test_conn",
            source_config={"schema": "public", "table": "test_table"},
        )

        result = op.execute(context)

        mock_hook.extract_data.assert_called_once_with("public", "test_table")
        assert result["rows"] == 1
        context["ti"].xcom_push.assert_any_call(
            key="extracted_rows", value=[{"id": 1, "name": "test"}]
        )

    def test_execute_no_rows(self):
        op = PostgresExtractOperator(
            task_id="extract",
            conn_id="test_conn",
            source_config={"schema": "public", "table": "empty_table"},
        )
        with patch("operators.postgres_extract_operator.EtlPostgresHook") as mock_cls:
            mock_hook = MagicMock()
            mock_cls.return_value = mock_hook
            mock_hook.extract_data.return_value = []

            context = {"ti": MagicMock()}
            result = op.execute(context)

            assert result["rows"] == 0
