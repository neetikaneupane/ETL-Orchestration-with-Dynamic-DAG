import pytest
from utils.data_quality import (
    check_not_null, check_unique, check_range, run_quality_checks,
)


class TestDataQuality:
    def test_check_not_null_passes(self):
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        count, cols = check_not_null(rows, ["id", "name"])
        assert count == 0

    def test_check_not_null_fails(self):
        rows = [{"id": 1, "name": None}, {"id": 2, "name": "b"}]
        count, cols = check_not_null(rows, ["name"])
        assert count == 1
        assert "name" in cols

    def test_check_unique_passes(self):
        rows = [{"id": 1}, {"id": 2}]
        total, dups = check_unique(rows, "id")
        assert dups == 0

    def test_check_unique_fails(self):
        rows = [{"id": 1}, {"id": 1}]
        total, dups = check_unique(rows, "id")
        assert dups == 1

    def test_check_range_passes(self):
        rows = [{"price": 10}, {"price": 50}]
        count, vals = check_range(rows, "price", min_val=0, max_val=100)
        assert count == 0

    def test_check_range_fails(self):
        rows = [{"price": -5}, {"price": 150}]
        count, vals = check_range(rows, "price", min_val=0, max_val=100)
        assert count == 2

    def test_run_quality_checks_all_pass(self):
        rows = [{"id": 1, "price": 10}, {"id": 2, "price": 50}]
        checks = [
            {"type": "not_null", "columns": ["id", "price"]},
            {"type": "unique", "column": "id"},
            {"type": "range", "column": "price", "min": 0, "max": 100},
        ]
        failures = run_quality_checks(rows, checks)
        assert len(failures) == 0

    def test_run_quality_checks_some_fail(self):
        rows = [{"id": 1, "price": None}, {"id": 1, "price": -5}]
        checks = [
            {"type": "not_null", "columns": ["price"]},
            {"type": "unique", "column": "id"},
            {"type": "range", "column": "price", "min": 0, "max": 100},
        ]
        failures = run_quality_checks(rows, checks)
        assert len(failures) >= 2
