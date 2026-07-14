from unittest.mock import MagicMock, patch

import pytest

from utils.dead_letter_queue import (
    identify_failing_rows,
    get_valid_rows,
    write_dlq_metadata,
    get_dlq_summary,
)


class TestIdentifyFailingRows:
    def test_not_null_failure(self):
        rows = [{"id": 1, "name": None}, {"id": 2, "name": "Alice"}]
        checks = [{"type": "not_null", "columns": ["name"]}]
        result = identify_failing_rows(rows, checks)
        assert len(result) == 1
        key = list(result.keys())[0]
        assert "name" in key
        assert len(result[key]) == 1
        assert result[key][0]["id"] == 1

    def test_not_null_all_pass(self):
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        checks = [{"type": "not_null", "columns": ["name"]}]
        result = identify_failing_rows(rows, checks)
        assert len(result) == 0

    def test_unique_failure(self):
        rows = [{"id": 1}, {"id": 1}, {"id": 2}]
        checks = [{"type": "unique", "column": "id"}]
        result = identify_failing_rows(rows, checks)
        assert len(result) == 1
        key = list(result.keys())[0]
        assert "duplicate" in key
        assert len(result[key]) == 2

    def test_unique_all_pass(self):
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        checks = [{"type": "unique", "column": "id"}]
        result = identify_failing_rows(rows, checks)
        assert len(result) == 0

    def test_range_failure(self):
        rows = [{"price": -5}, {"price": 50}, {"price": 200}]
        checks = [{"type": "range", "column": "price", "min": 0, "max": 100}]
        result = identify_failing_rows(rows, checks)
        total_failing = sum(len(v) for v in result.values())
        assert total_failing == 2
        all_values = []
        for v in result.values():
            all_values.extend(v)
        prices = sorted([r["price"] for r in all_values])
        assert prices == [-5, 200]

    def test_range_all_pass(self):
        rows = [{"price": 10}, {"price": 50}]
        checks = [{"type": "range", "column": "price", "min": 0, "max": 100}]
        result = identify_failing_rows(rows, checks)
        assert len(result) == 0

    def test_range_non_numeric(self):
        rows = [{"price": "abc"}, {"price": 50}]
        checks = [{"type": "range", "column": "price", "min": 0, "max": 100}]
        result = identify_failing_rows(rows, checks)
        assert len(result) == 1
        key = list(result.keys())[0]
        assert "non-numeric" in key

    def test_multiple_checks(self):
        rows = [
            {"id": 1, "name": None, "price": 50},
            {"id": 1, "name": "Bob", "price": 200},
            {"id": 3, "name": "Charlie", "price": 30},
        ]
        checks = [
            {"type": "not_null", "columns": ["name"]},
            {"type": "unique", "column": "id"},
            {"type": "range", "column": "price", "min": 0, "max": 100},
        ]
        result = identify_failing_rows(rows, checks)
        assert len(result) >= 2

    def test_empty_rows(self):
        checks = [{"type": "not_null", "columns": ["name"]}]
        result = identify_failing_rows([], checks)
        assert len(result) == 0

    def test_unknown_check_type(self):
        rows = [{"id": 1}]
        checks = [{"type": "unknown_check"}]
        result = identify_failing_rows(rows, checks)
        assert len(result) == 0


class TestGetValidRows:
    def test_filters_out_failing_rows(self):
        rows = [
            {"id": 1, "name": None},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": None},
        ]
        checks = [{"type": "not_null", "columns": ["name"]}]
        valid = get_valid_rows(rows, checks)
        assert len(valid) == 1
        assert valid[0]["id"] == 2

    def test_all_valid(self):
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        checks = [{"type": "not_null", "columns": ["name"]}]
        valid = get_valid_rows(rows, checks)
        assert len(valid) == 2

    def test_all_failing(self):
        rows = [{"id": 1, "name": None}, {"id": 2, "name": None}]
        checks = [{"type": "not_null", "columns": ["name"]}]
        valid = get_valid_rows(rows, checks)
        assert len(valid) == 0

    def test_empty_rows(self):
        checks = [{"type": "not_null", "columns": ["name"]}]
        valid = get_valid_rows([], checks)
        assert len(valid) == 0


class TestWriteDlqMetadata:
    @patch("utils.db.get_pg_conn")
    def test_write_metadata(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        write_dlq_metadata(
            pipeline_id="test_pipeline",
            execution_date="2024-01-01",
            dag_run_id="run-123",
            failure_type="not_null: column 'name' is null",
            failure_message="1 rows failed not_null check",
            row_count=1,
            s3_key="dlq/test/2024-01-01/data.jsonl",
            s3_bucket="dlq-data",
        )

        mock_conn.cursor.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("utils.db.get_pg_conn")
    def test_write_metadata_db_error(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            write_dlq_metadata(
                pipeline_id="test_pipeline",
                execution_date="2024-01-01",
                dag_run_id=None,
                failure_type="test",
                failure_message="test",
                row_count=0,
            )

        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()


class TestGetDlqSummary:
    @patch("utils.db.get_pg_conn")
    def test_get_summary(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [
            ("test_pipeline", "not_null", 10, 3),
        ]
        mock_cursor.description = [
            ("pipeline_id",),
            ("failure_type",),
            ("total_rows",),
            ("entry_count",),
        ]

        result = get_dlq_summary(pipeline_id="test_pipeline", hours=24)
        assert len(result) == 1
        assert result[0]["pipeline_id"] == "test_pipeline"
        assert result[0]["total_rows"] == 10

    @patch("utils.db.get_pg_conn")
    def test_get_summary_no_filter(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [
            ("pipeline_id",),
            ("failure_type",),
            ("total_rows",),
            ("entry_count",),
        ]

        result = get_dlq_summary()
        assert len(result) == 0
