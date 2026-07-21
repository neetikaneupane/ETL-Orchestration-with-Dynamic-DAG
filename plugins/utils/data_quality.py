"""
data_quality.py
===============
Data quality checks and validation utilities for the transform step.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a data quality check fails."""

    def __init__(
        self, message: str, failures: Optional[List[str]] = None, row_count: int = 0
    ):
        super().__init__(message)
        self.failures = failures or []
        self.row_count = row_count


def check_not_null(
    rows: List[Dict[str, Any]], columns: List[str]
) -> Tuple[int, List[str]]:
    """Check that specified columns have no null values.

    Returns (failed_rows_count, list of column names with nulls).
    """
    failed_cols = set()
    for row in rows:
        for col in columns:
            if row.get(col) is None:
                failed_cols.add(col)
    if failed_cols:
        log.warning(f"Null values found in columns: {failed_cols}")
    return len(failed_cols), list(failed_cols)


def check_unique(rows: List[Dict[str, Any]], column: str) -> Tuple[int, int]:
    """Check that values in a column are unique.

    Returns (total_rows, duplicate_count).
    """
    values = [row.get(column) for row in rows]
    unique = set(values)
    duplicates = len(values) - len(unique)
    if duplicates:
        log.warning(f"Found {duplicates} duplicate values in column '{column}'")
    return len(values), duplicates


def check_range(
    rows: List[Dict[str, Any]],
    column: str,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> Tuple[int, List[Any]]:
    """Check that column values fall within a range.

    Returns (out_of_range_count, list of offending values).
    """
    outliers = []
    for row in rows:
        val = row.get(column)
        if val is None:
            continue
        try:
            fval = float(val)
            if min_val is not None and fval < min_val:
                outliers.append(val)
            elif max_val is not None and fval > max_val:
                outliers.append(val)
        except (TypeError, ValueError):
            outliers.append(val)

    if outliers:
        log.warning(f"Found {len(outliers)} out-of-range values in column '{column}'")
    return len(outliers), outliers


def run_quality_checks(
    rows: List[Dict[str, Any]],
    checks: List[Dict[str, Any]],
) -> List[str]:
    """Run a list of quality checks against the data.

    Each check is a dict:
        {"type": "not_null", "columns": ["col1", "col2"]}
        {"type": "unique", "column": "id"}
        {"type": "range", "column": "price", "min": 0, "max": 10000}

    Returns a list of failure messages. Empty list = all passed.
    """
    failures = []

    for check in checks:
        ctype = check.get("type")
        try:
            if ctype == "not_null":
                count, cols = check_not_null(rows, check.get("columns", []))
                if count:
                    failures.append(f"not_null: {count} columns with nulls: {cols}")
            elif ctype == "unique":
                total, dups = check_unique(rows, check.get("column", ""))
                if dups:
                    failures.append(f"unique: {dups} duplicates in '{check['column']}'")
            elif ctype == "range":
                count, vals = check_range(
                    rows, check["column"], check.get("min"), check.get("max")
                )
                if count:
                    failures.append(
                        f"range: {count} values outside [{check.get('min')}, {check.get('max')}] "
                        f"in '{check['column']}': {vals[:5]}"
                    )
        except Exception as e:
            failures.append(f"{ctype} check failed with error: {e}")

    if failures:
        log.warning(f"Data quality checks failed: {failures}")
    else:
        log.info(f"All {len(checks)} data quality checks passed")

    return failures
