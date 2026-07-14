import logging
import time
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from utils.data_quality import run_quality_checks, DataQualityError

log = logging.getLogger(__name__)


class TransformOperator(BaseOperator):
    """Applies transformations and data quality checks to extracted data."""

    def __init__(
        self,
        transform_config: Optional[Dict[str, Any]] = None,
        dlq_config: Optional[Dict[str, Any]] = None,
        pipeline_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.transform_config = transform_config or {}
        self.dlq_config = dlq_config or {}
        self.pipeline_id = pipeline_id

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
        failure_action = self.transform_config.get("quality_failure_action", "raise")
        dlq_row_count = 0
        dlq_failure_count = 0

        if quality_checks and transformed:
            if (
                failure_action == "dlq"
                and self.dlq_config
                and self.dlq_config.get("enabled")
            ):
                from utils.dead_letter_queue import (
                    identify_failing_rows,
                    get_valid_rows,
                    write_dlq_metadata,
                )
                from destinations.s3_dlq_destination import S3DlqDestination

                failing = identify_failing_rows(transformed, quality_checks)
                ti.xcom_push(key="quality_failures", value=list(failing.keys()))

                if failing:
                    all_failing_rows = []
                    for failure_msg, failing_rows in failing.items():
                        all_failing_rows.extend(failing_rows)
                        write_dlq_metadata(
                            pipeline_id=self.pipeline_id,
                            execution_date=context.get("ds"),
                            dag_run_id=context.get("run_id"),
                            failure_type=failure_msg,
                            failure_message=failure_msg,
                            row_count=len(failing_rows),
                        )
                        log.warning(
                            f"DLQ: routed {len(failing_rows)} rows: {failure_msg}"
                        )

                    dlq_failure_count = len(failing)
                    dlq_row_count = len(all_failing_rows)

                    dlq_dest = S3DlqDestination(self.dlq_config)
                    try:
                        s3_key = dlq_dest.write(
                            all_failing_rows,
                            pipeline_id=self.pipeline_id,
                            execution_date=context.get("ds"),
                        )
                        if s3_key:
                            from utils.dead_letter_queue import write_dlq_metadata

                            write_dlq_metadata(
                                pipeline_id=self.pipeline_id,
                                execution_date=context.get("ds"),
                                dag_run_id=context.get("run_id"),
                                failure_type="dlq_s3_write",
                                failure_message=f"Wrote {dlq_row_count} rows to {s3_key}",
                                row_count=dlq_row_count,
                                s3_key=s3_key,
                                s3_bucket=self.dlq_config.get("bucket"),
                            )
                    finally:
                        dlq_dest.close()

                    transformed = get_valid_rows(transformed, quality_checks)
                    log.info(
                        f"DLQ: {dlq_row_count} failed rows routed, "
                        f"{len(transformed)} valid rows continue"
                    )
                else:
                    log.info("DLQ mode: all rows passed quality checks")
            else:
                failures = run_quality_checks(transformed, quality_checks)
                ti.xcom_push(key="quality_failures", value=failures)
                if failures:
                    if failure_action == "raise":
                        raise DataQualityError(
                            f"{len(failures)} quality check(s) failed",
                            failures=failures,
                            row_count=len(transformed),
                        )
                    log.warning(f"Quality failures (non-blocking): {failures}")

        ti.xcom_push(key="transformed_rows", value=transformed)
        ti.xcom_push(key="transform_duration", value=duration)
        ti.xcom_push(key="transformed_columns", value=transformed_columns)
        ti.xcom_push(key="dlq_row_count", value=dlq_row_count)
        ti.xcom_push(key="dlq_failure_count", value=dlq_failure_count)

        log.info(f"Transformed {len(rows)} -> {len(transformed)} rows in {duration}s")
        return {
            "rows_before": len(rows),
            "rows_after": len(transformed),
            "duration": duration,
            "dlq_row_count": dlq_row_count,
            "dlq_failure_count": dlq_failure_count,
        }
