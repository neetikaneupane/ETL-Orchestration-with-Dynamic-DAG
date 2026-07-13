from unittest.mock import MagicMock, patch


class TestSlaMonitor:
    @patch("dags.sla_monitor.get_pg_conn")
    def test_check_sla_breaches_none(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        from dags.sla_monitor import check_sla_breaches
        result = check_sla_breaches(**{})

        assert result == 0

    @patch("dags.sla_monitor.notify_sla_breach")
    @patch("dags.sla_monitor.get_pg_conn")
    def test_check_sla_breaches_found(self, mock_get_conn, mock_notify):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("pipeline_1", "2024-01-01", 7200, 60),
        ]

        from dags.sla_monitor import check_sla_breaches
        result = check_sla_breaches(**{})

        assert result == 1
        mock_notify.assert_called_once_with("pipeline_1", 60.0)

    @patch("dags.sla_monitor.get_pg_conn")
    def test_update_health_summary(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        from dags.sla_monitor import update_health_summary
        update_health_summary(**{})

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch("dags.sla_monitor.notify_consecutive_failures")
    @patch("dags.sla_monitor.get_pg_conn")
    def test_alert_on_consecutive_failures_none(self, mock_get_conn, mock_notify):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        from dags.sla_monitor import alert_on_consecutive_failures
        result = alert_on_consecutive_failures(**{})

        assert result == 0
        mock_notify.assert_not_called()

    @patch("dags.sla_monitor.notify_consecutive_failures")
    @patch("dags.sla_monitor.get_pg_conn")
    def test_alert_on_consecutive_failures_found(self, mock_get_conn, mock_notify):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("pipeline_1", 5),
        ]

        from dags.sla_monitor import alert_on_consecutive_failures
        result = alert_on_consecutive_failures(**{})

        assert result == 1
        mock_notify.assert_called_once_with("pipeline_1", 5)
