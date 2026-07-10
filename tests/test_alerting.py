import pytest
from unittest.mock import MagicMock, patch


class TestAlerting:
    @patch("utils.alerting._get_alert_config")
    def test_send_slack_alert_success(self, mock_config):
        mock_config.return_value = {"slack_webhook_url": "https://hooks.slack.com/test"}

        with patch("utils.alerting.httpx.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.raise_for_status = MagicMock()
            mock_post.return_value = mock_resp

            from utils.alerting import send_slack_alert
            result = send_slack_alert("test message")

            assert result is True
            mock_post.assert_called_once()

    @patch("utils.alerting._get_alert_config")
    def test_send_slack_alert_no_webhook(self, mock_config):
        mock_config.return_value = {}

        from utils.alerting import send_slack_alert
        result = send_slack_alert("test message")

        assert result is False

    @patch("utils.alerting._get_alert_config")
    def test_send_slack_alert_http_error(self, mock_config):
        mock_config.return_value = {"slack_webhook_url": "https://hooks.slack.com/test"}

        with patch("utils.alerting.httpx.post") as mock_post:
            mock_post.side_effect = Exception("Connection error")

            from utils.alerting import send_slack_alert
            result = send_slack_alert("test message")

            assert result is False

    @patch("utils.alerting._get_alert_config")
    def test_send_email_alert_success(self, mock_config):
        mock_config.return_value = {"alert_email_to": "test@example.com"}

        with patch("airflow.utils.email.send_email") as mock_email:
            from utils.alerting import send_email_alert
            result = send_email_alert("subject", "<p>body</p>")

            assert result is True
            mock_email.assert_called_once()

    @patch("utils.alerting._get_alert_config")
    def test_send_email_alert_no_recipient(self, mock_config):
        mock_config.return_value = {}

        with patch("airflow.utils.email.send_email") as mock_email:
            from utils.alerting import send_email_alert
            result = send_email_alert("subject", "<p>body</p>")

            assert result is True
            mock_email.assert_called_once()

    @patch("utils.alerting.send_email_alert", return_value=True)
    @patch("utils.alerting.send_slack_alert", return_value=True)
    def test_notify_pipeline_failure(self, mock_slack, mock_email):
        from utils.alerting import notify_pipeline_failure
        notify_pipeline_failure("pipeline_1", "task_1", "ValueError", "run_url")

        mock_slack.assert_called_once()
        mock_email.assert_called_once()

    @patch("utils.alerting.send_email_alert", return_value=True)
    @patch("utils.alerting.send_slack_alert", return_value=True)
    def test_notify_sla_breach(self, mock_slack, mock_email):
        from utils.alerting import notify_sla_breach
        notify_sla_breach("pipeline_1", 15.5)

        mock_slack.assert_called_once()
        mock_email.assert_called_once()

    @patch("utils.alerting.send_email_alert", return_value=True)
    @patch("utils.alerting.send_slack_alert", return_value=True)
    def test_notify_consecutive_failures(self, mock_slack, mock_email):
        from utils.alerting import notify_consecutive_failures
        notify_consecutive_failures("pipeline_1", 5)

        mock_slack.assert_called_once()
        mock_email.assert_called_once()
