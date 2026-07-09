import logging
import time
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from utils.data_quality import run_quality_checks

log = logging.getLogger(__name__)


class TransformOperator(BaseOperator):
    """Applies transformations and data quality checks to extracted data."""

    def __init__(self, transform_config: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)
        self.transform_config = transform_config or {}

    def execute(self, context):
        ti = context["ti"]
        rows = ti.xcom_pull(task_ids="extract", key="extracted_rows")
        columns = ti.xcom_pull(task_ids="extract", key="columns")

        if not rows:
            log.info("No rows to transform.")
            ti.xcom_push(key="transformed_rows", value=[])
            ti.xcom_push(key="transform_duration", value=0)
            return {"rows": 0, "columns": columns}

        started_at = time.time()
        transformed = []
        drop_columns = self.transform_config.get("drop_columns", [])
        rename_map = self.transform_config.get("rename_columns", {})
        filters = self.transform_config.get("filters", [])

        for row in rows:
            row_copy = dict(row)
            for col in drop_columns:
                row_copy.pop(col, None)
            for old_name, new_name in rename_map.items():
                if old_name in row_copy:
                    row_copy[new_name] = row_copy.pop(old_name)

            if filters:
                include = True
                for f in filters:
                    col = f.get("column")
                    op = f.get("op", "eq")
                    val = f.get("value")
                    cell = row_copy.get(col)
                    if op == "eq" and cell != val:
                        include = False
                    elif op == "neq" and cell == val:
                        include = False
                    elif op == "gt" and not (cell is not None and cell > val):
                        include = False
                    elif op == "lt" and not (cell is not None and cell < val):
                        include = False
                if not include:
                    continue

            transformed.append(row_copy)

        duration = round(time.time() - started_at, 2)
        transformed_columns = list(transformed[0].keys()) if transformed else columns

        quality_checks = self.transform_config.get("quality_checks", [])
        if quality_checks and transformed:
            failures = run_quality_checks(transformed, quality_checks)
            ti.xcom_push(key="quality_failures", value=failures)
            if failures:
                log.warning(f"Quality failures: {failures}")

        ti.xcom_push(key="transformed_rows", value=transformed)
        ti.xcom_push(key="transform_duration", value=duration)
        ti.xcom_push(key="transformed_columns", value=transformed_columns)

        log.info(f"Transformed {len(rows)} -> {len(transformed)} rows in {duration}s")
        return {"rows_before": len(rows), "rows_after": len(transformed), "duration": duration}
