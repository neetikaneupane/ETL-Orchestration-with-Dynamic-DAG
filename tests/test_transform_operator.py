from unittest.mock import MagicMock, patch

import pytest

from operators.transform_operator import TransformOperator
from utils.data_quality import DataQualityError


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
            transform_config={
                "filters": [{"column": "type", "op": "eq", "value": "A"}]
            },
        )
        result = op.execute(context)

        assert result["rows_after"] == 2

    def test_execute_filter_neq(self):
        rows_input = [
            {"status": "active"},
            {"status": "inactive"},
            {"status": "active"},
        ]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["status"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={
                "filters": [{"column": "status", "op": "neq", "value": "active"}]
            },
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
            transform_config={
                "filters": [{"column": "amount", "op": "gt", "value": 20}]
            },
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
            transform_config={
                "filters": [{"column": "amount", "op": "lt", "value": 50}]
            },
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

    def test_execute_quality_failure_raises_by_default(self):
        rows_input = [{"id": 1, "name": None}, {"id": 2, "name": "test"}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
            },
        )

        with pytest.raises(DataQualityError) as exc_info:
            op.execute(context)

        assert "1 quality check(s) failed" in str(exc_info.value)
        assert len(exc_info.value.failures) > 0
        assert exc_info.value.row_count == 2

    def test_execute_quality_failure_warn_mode(self):
        rows_input = [{"id": 1, "name": None}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
                "quality_failure_action": "warn",
            },
        )
        result = op.execute(context)

        assert result["rows_after"] == 1

    def test_execute_quality_pass(self):
        rows_input = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        context = {
            "ti": MagicMock(),
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
            },
        )
        result = op.execute(context)

        assert result["rows_after"] == 2


class TestTransformOperatorDlq:
    @patch("destinations.s3_dlq_destination.S3DlqDestination")
    @patch("utils.dead_letter_queue.write_dlq_metadata")
    def test_dlq_mode_routes_failing_rows(self, mock_write_meta, mock_dlq_dest_class):
        mock_dlq_dest = MagicMock()
        mock_dlq_dest_class.return_value = mock_dlq_dest
        mock_dlq_dest.write.return_value = "dlq/test/2024-01-01/data.jsonl"

        rows_input = [
            {"id": 1, "name": None},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": None},
        ]
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "run_id": "run-123",
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            pipeline_id="test_pipeline",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
                "quality_failure_action": "dlq",
            },
            dlq_config={"enabled": True, "bucket": "dlq-data", "prefix": "dlq/"},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1
        assert result["dlq_row_count"] == 2
        assert result["dlq_failure_count"] == 1
        mock_dlq_dest.write.assert_called_once()
        mock_dlq_dest.close.assert_called_once()

    @patch("destinations.s3_dlq_destination.S3DlqDestination")
    @patch("utils.dead_letter_queue.write_dlq_metadata")
    def test_dlq_mode_all_pass(self, mock_write_meta, mock_dlq_dest_class):
        rows_input = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "run_id": "run-123",
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            pipeline_id="test_pipeline",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
                "quality_failure_action": "dlq",
            },
            dlq_config={"enabled": True, "bucket": "dlq-data", "prefix": "dlq/"},
        )
        result = op.execute(context)

        assert result["rows_after"] == 2
        assert result["dlq_row_count"] == 0
        assert result["dlq_failure_count"] == 0
        mock_dlq_dest_class.assert_not_called()

    def test_dlq_mode_disabled_falls_back_to_warn(self):
        rows_input = [{"id": 1, "name": None}]
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "run_id": "run-123",
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            pipeline_id="test_pipeline",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
                "quality_failure_action": "dlq",
            },
            dlq_config={"enabled": False},
        )
        result = op.execute(context)

        assert result["rows_after"] == 1
        assert result["dlq_row_count"] == 0

    def test_dlq_mode_no_dlq_config_falls_back_to_warn(self):
        rows_input = [{"id": 1, "name": None}]
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "run_id": "run-123",
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            pipeline_id="test_pipeline",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
                "quality_failure_action": "dlq",
            },
        )
        result = op.execute(context)

        assert result["rows_after"] == 1
        assert result["dlq_row_count"] == 0

    @patch("destinations.s3_dlq_destination.S3DlqDestination")
    @patch("utils.dead_letter_queue.write_dlq_metadata")
    def test_dlq_mode_s3_write_failure_still_records_metadata(
        self, mock_write_meta, mock_dlq_dest_class
    ):
        mock_dlq_dest = MagicMock()
        mock_dlq_dest_class.return_value = mock_dlq_dest
        mock_dlq_dest.write.side_effect = Exception("S3 write failed")

        rows_input = [{"id": 1, "name": None}]
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "run_id": "run-123",
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            pipeline_id="test_pipeline",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
                "quality_failure_action": "dlq",
            },
            dlq_config={"enabled": True, "bucket": "dlq-data", "prefix": "dlq/"},
        )

        with pytest.raises(Exception, match="S3 write failed"):
            op.execute(context)

        assert mock_write_meta.call_count == 1
        mock_dlq_dest.close.assert_called_once()

    def test_dlq_returns_dlq_metrics_in_result(self):
        rows_input = [{"id": 1, "name": "Alice"}]
        context = {
            "ti": MagicMock(),
            "ds": "2024-01-01",
            "run_id": "run-123",
        }
        context["ti"].xcom_pull.side_effect = lambda task_ids, key: {
            ("extract", "extracted_rows"): rows_input,
            ("extract", "columns"): ["id", "name"],
        }.get((task_ids, key), [])

        op = TransformOperator(
            task_id="transform",
            pipeline_id="test_pipeline",
            transform_config={
                "quality_checks": [{"type": "not_null", "columns": ["name"]}],
                "quality_failure_action": "dlq",
            },
            dlq_config={"enabled": True, "bucket": "dlq-data", "prefix": "dlq/"},
        )
        result = op.execute(context)

        assert "dlq_row_count" in result
        assert "dlq_failure_count" in result
        assert result["dlq_row_count"] == 0
        assert result["dlq_failure_count"] == 0
