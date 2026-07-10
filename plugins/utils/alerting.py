"""
alerting.py
===========
Send alerts via Slack webhook or email when pipelines fail
or SLA breaches are detected.
"""

import json
import logging
from typing import Any, Dict, Optional

import httpx
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


def _get_alert_config() -> Dict[str, Any]:
    """Read alert configuration from Airflow variables or defaults."""
    try:
        from airflow.models import Variable
        return Variable.get("alert_config", deserialize_json=True) or {}
    except (ValueError, KeyError):
        return {}


def send_slack_alert(message: str, webhook_url: Optional[str] = None) -> bool:
    """Send a message to a Slack channel via Incoming Webhook."""
    if not webhook_url:
        config = _get_alert_config()
        webhook_url = config.get("slack_webhook_url")

    if not webhook_url:
        log.warning("No Slack webhook URL configured, skipping alert")
        return False

    try:
        payload = {"text": message}
        resp = httpx.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info(f"Slack alert sent: {message[:80]}...")
        return True
    except Exception as e:
        log.error(f"Failed to send Slack alert: {e}")
        return False


def send_email_alert(subject: str, body: str, to: Optional[str] = None) -> bool:
    """Send an email alert using Airflow's email capabilities."""
    try:
        from airflow.utils.email import send_email

        config = _get_alert_config()
        recipients = to or config.get("alert_email_to", "data-engineering@example.com")

        send_email(to=recipients, subject=subject, html_content=body)
        log.info(f"Email alert sent to {recipients}: {subject}")
        return True
    except Exception as e:
        log.error(f"Failed to send email alert: {e}")
        return False


def notify_pipeline_failure(
    pipeline_id: str,
    task_id: str,
    error: str,
    run_url: Optional[str] = None,
) -> None:
    """Send alerts for a pipeline task failure."""
    subject = f"ETL Failure: {pipeline_id}.{task_id}"
    message = (
        f":red_circle: *ETL Pipeline Failure*\n"
        f"*Pipeline:* `{pipeline_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Error:* `{error}`\n"
        f"*Run:* {run_url or 'N/A'}"
    )

    send_slack_alert(message)
    send_email_alert(subject, f"<pre>{message}</pre>")


def notify_sla_breach(pipeline_id: str, breach_minutes: float) -> None:
    """Send alerts for an SLA breach."""
    subject = f"SLA Breach: {pipeline_id} exceeded by {breach_minutes}m"
    message = (
        f":warning: *SLA Breach*\n"
        f"*Pipeline:* `{pipeline_id}`\n"
        f"*Exceeded by:* `{breach_minutes}m`"
    )

    send_slack_alert(message)
    send_email_alert(subject, f"<pre>{message}</pre>")


def notify_consecutive_failures(pipeline_id: str, count: int) -> None:
    """Send alerts for consecutive pipeline failures."""
    subject = f"Critical: {pipeline_id} failed {count} times consecutively"
    message = (
        f":fire: *Consecutive Failures*\n"
        f"*Pipeline:* `{pipeline_id}`\n"
        f"*Failures:* `{count} in last 6 hours`"
    )

    send_slack_alert(message)
    send_email_alert(subject, f"<pre>{message}</pre>")
