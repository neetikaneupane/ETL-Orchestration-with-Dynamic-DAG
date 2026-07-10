import pytest
from unittest.mock import MagicMock

from operators.transform_operator import TransformOperator


class TestTransformOperator:
    def test_execute_no_rows(self):
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.return_value = []

        op = TransformOperator(task_id="transform")
        result = op.execute(context)

        assert result["rows"] == 0

    def test_execute_drop_columns(self):
        rows_input = [{"id": 1, "name": "test", "secret": "sensitive"}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name", "secret"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"drop_columns": ["secret"]},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1
        assert result["rows_before"] == 1

    def test_execute_rename_columns(self):
        rows_input = [{"old_name": "value"}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["old_name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"rename_columns": {"old_name": "new_name"}},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1

    def test_execute_filter_eq(self):
        rows_input = [{"type": "A"}, {"type": "B"}, {"type": "A"}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["type"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"filters": [{"column": "type", "op": "eq", "value": "A"}]},
        )
        result = op.execute(context)

        assert result["rows_after"] == 2

    def test_execute_filter_neq(self):
        rows_input = [{"status": "active"}, {"status": "inactive"}, {"status": "active"}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["status"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"filters": [{"column": "status", "op": "neq", "value": "active"}]},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1

    def test_execute_filter_gt(self):
        rows_input = [{"amount": 10}, {"amount": 50}, {"amount": 100}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["amount"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"filters": [{"column": "amount", "op": "gt", "value": 20}]},
        )
        result = op.execute(context)

        assert result["rows_after"] == 2

    def test_execute_filter_lt(self):
        rows_input = [{"amount": 10}, {"amount": 50}, {"amount": 100}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["amount"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"filters": [{"column": "amount", "op": "lt", "value": 50}]},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1

    def test_execute_drop_nonexistent_column(self):
        rows_input = [{"id": 1, "name": "test"}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"drop_columns": ["nonexistent"]},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1

    def test_execute_rename_nonexistent_column(self):
        rows_input = [{"id": 1}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={"rename_columns": {"old": "new"}},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1
