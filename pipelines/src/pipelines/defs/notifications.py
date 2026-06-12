"""
Job failure email notifications via the Gmail API.

Sends an email whenever a scheduled job run fails. Manual runs (repairs,
one-off materializations) are intentionally skipped — you triggered those
yourself and already know they failed.

Auth: a Google service account with domain-wide delegation sends "as" a real
Workspace user (no SMTP password). The service account JSON key is mounted as
a Docker secret.

Configuration via env vars (.env / docker-compose):
    GMAIL_SA_KEY_PATH  /run/secrets/gmail_service_account  (service account JSON)
    GMAIL_SEND_AS      alerts@example.org   (Workspace user the SA impersonates)
    ALERT_EMAIL        admin@example.org   (switch to itstaff@ at go-live)
    DAGSTER_UI_URL     http://localhost:3000     (update to VM IP at go-live)
"""
import base64
import os
import traceback
from datetime import datetime, timezone
from email.mime.text import MIMEText

import requests
from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

_MONITORED_JOBS = {"ks_daily_job", "ks_monthly_job", "hubspot_nightly_job"}
_GMAIL_SCOPES   = ["https://www.googleapis.com/auth/gmail.send"]
_GMAIL_SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"


def _send_alert_email(subject: str, body: str) -> None:
    """Send an email via the Gmail API using the delegated service account."""
    key_path = os.environ.get("GMAIL_SA_KEY_PATH", "/run/secrets/gmail_service_account")
    send_as  = os.environ.get("GMAIL_SEND_AS", "alerts@example.org")
    alert_to = os.environ.get("ALERT_EMAIL", "")

    if not alert_to:
        raise RuntimeError("ALERT_EMAIL not set — no recipient configured")
    if not os.path.exists(key_path):
        raise RuntimeError(f"Service account key not found at {key_path}")

    # Imported lazily so the module loads even if google-auth is missing
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request

    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=_GMAIL_SCOPES
    ).with_subject(send_as)
    creds.refresh(Request())

    msg            = MIMEText(body)
    msg["to"]      = alert_to
    msg["from"]    = send_as
    msg["subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    resp = requests.post(
        _GMAIL_SEND_URL,
        headers={"Authorization": f"Bearer {creds.token}"},
        json={"raw": raw},
        timeout=15,
    )
    resp.raise_for_status()


@run_failure_sensor(
    name="job_failure_email_alert",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def job_failure_email_sensor(context: RunFailureSensorContext) -> None:
    run = context.dagster_run

    # Only alert on the two scheduled jobs — ignore repair assets and manual runs
    if run.job_name not in _MONITORED_JOBS:
        return

    if "dagster/schedule_name" not in run.tags:
        context.log.info(f"Skipping alert for non-scheduled run {run.run_id[:8]}")
        return

    schedule_name = run.tags.get("dagster/schedule_name", "unknown")
    partition     = run.tags.get("dagster/partition", "no partition")
    ui_base       = os.environ.get("DAGSTER_UI_URL", "http://localhost:3000")
    run_url       = f"{ui_base}/runs/{run.run_id}"
    failed_at     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    error_msg = ""
    try:
        error_msg = str(context.failure_event.event_specific_data.error)
    except Exception:
        error_msg = "(see Dagster UI for full error)"

    subject = f"[Dagster] {run.job_name} FAILED — {partition}"

    body = f"""A scheduled Dagster job has failed and requires attention.

Job:        {run.job_name}
Schedule:   {schedule_name}
Partition:  {partition}
Failed at:  {failed_at}
Run ID:     {run.run_id}

Error:
{error_msg}

View full logs and step details:
{run_url}

---
This alert was sent because the run was triggered by a schedule.
Manual runs do not generate alerts.
"""

    try:
        _send_alert_email(subject, body)
        context.log.info(f"Failure alert sent to {os.environ.get('ALERT_EMAIL', '(no recipient)')}")
    except Exception:
        context.log.error(f"Failed to send alert email:\n{traceback.format_exc()}")
