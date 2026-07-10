import pytest
from unittest.mock import MagicMock, patch


class TestRetryUtils:
    def test_compute_backoff_delay_default(self):
        from utils.retry_utils import compute_backoff_delay

        assert compute_backoff_delay(1) == 60
        assert compute_backoff_delay(2) == 120
        assert compute_backoff_delay(3) == 240
        assert compute_backoff_delay(4) == 480

    def test_compute_backoff_delay_with_policy(self):
        from utils.retry_utils import compute_backoff_delay

        policy = {"base_delay": 30, "max_delay": 600}
        assert compute_backoff_delay(1, policy) == 30
        assert compute_backoff_delay(2, policy) == 60
        assert compute_backoff_delay(3, policy) == 120

    def test_compute_backoff_delay_capped(self):
        from utils.retry_utils import compute_backoff_delay

        assert compute_backoff_delay(20) == 3600

    def test_compute_backoff_delay_custom_caps(self):
        from utils.retry_utils import compute_backoff_delay

        assert compute_backoff_delay(10, base_delay=60, max_delay=500) == 500

    @patch("utils.retry_utils.get_pg_conn")
    def test_get_retry_policy_found(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (
            "exponential", 5, 60, 3600, True
        )

        from utils.retry_utils import get_retry_policy
        result = get_retry_policy("pipeline_1", "OperationalError")

        assert result is not None
        assert result["strategy"] == "exponential"
        assert result["max_retries"] == 5
        assert result["alert_on_failure"] is True

    @patch("utils.retry_utils.get_pg_conn")
    def test_get_retry_policy_not_found(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        from utils.retry_utils import get_retry_policy
        result = get_retry_policy("pipeline_1", "UnknownError")

        assert result is None

    @patch("utils.retry_utils.get_pg_conn")
    def test_get_retry_policy_db_error(self, mock_get_conn):
        mock_get_conn.side_effect = Exception("Connection failed")

        from utils.retry_utils import get_retry_policy
        result = get_retry_policy("pipeline_1", "OperationalError")

        assert result is None

    @patch("utils.retry_utils.get_retry_policy")
    def test_get_effective_max_retries_with_policy(self, mock_policy):
        mock_policy.return_value = {"max_retries": 7}

        from utils.retry_utils import get_effective_max_retries
        result = get_effective_max_retries("pipeline_1", "OperationalError")

        assert result == 7

    @patch("utils.retry_utils.get_retry_policy")
    def test_get_effective_max_retries_no_policy(self, mock_policy):
        mock_policy.return_value = None

        from utils.retry_utils import get_effective_max_retries
        result = get_effective_max_retries("pipeline_1", "OperationalError")

        assert result == 3

    @patch("utils.retry_utils.get_retry_policy")
    def test_get_effective_max_retries_custom_default(self, mock_policy):
        mock_policy.return_value = None

        from utils.retry_utils import get_effective_max_retries
        result = get_effective_max_retries("pipeline_1", "OperationalError", default_retries=5)

        assert result == 5
