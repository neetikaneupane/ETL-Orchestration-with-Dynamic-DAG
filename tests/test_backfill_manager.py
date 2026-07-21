from unittest.mock import MagicMock, patch
from datetime import datetime


class TestBackfillManager:
    @patch("dags.backfill_manager.get_pg_conn")
    def test_process_no_pending_requests(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        from dags.backfill_manager import process_backfill_requests

        result = process_backfill_requests(**{})

        assert result == 0

    @patch("dags.backfill_manager.DagRun")
    @patch("dags.backfill_manager.DagBag")
    @patch("dags.backfill_manager.get_pg_conn")
    def test_process_backfill_requests(
        self, mock_get_conn, mock_dag_bag_cls, mock_dag_run
    ):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        req_id = 1
        pipeline_id = "test_pipeline"
        start_date = datetime(2024, 1, 1).date()
        end_date = datetime(2024, 1, 2).date()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (req_id, pipeline_id, start_date, end_date)
        ]

        mock_dag_bag = MagicMock()
        mock_dag_bag_cls.return_value = mock_dag_bag
        mock_dag_bag.dags = {pipeline_id: MagicMock()}

        mock_dag_run.find.return_value = []

        from dags.backfill_manager import process_backfill_requests

        result = process_backfill_requests(**{})

        assert result == 2

    @patch("dags.backfill_manager.get_pg_conn")
    def test_process_dag_not_found(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (
                1,
                "missing_pipeline",
                datetime(2024, 1, 1).date(),
                datetime(2024, 1, 1).date(),
            )
        ]

        mock_dag_bag = MagicMock()
        with patch("dags.backfill_manager.DagBag") as mock_dag_bag_cls:
            mock_dag_bag_cls.return_value = mock_dag_bag
            mock_dag_bag.dags = {}

            from dags.backfill_manager import process_backfill_requests

            result = process_backfill_requests(**{})

        assert result == 0
        with mock_conn.cursor() as cur:
            cur.execute.assert_called()
            update_call = [c for c in cur.execute.call_args_list if "UPDATE" in str(c)]
            assert len(update_call) > 0
