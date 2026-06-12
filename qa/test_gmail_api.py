"""Test Gmail API send via service account with domain-wide delegation."""
import base64
import sys
from email.mime.text import MIMEText

import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request

KEY_FILE       = "/tmp/gmail_sa.json"
DELEGATED_USER = "alerts@example.org"      # real Workspace user the SA sends AS
TO             = "admin@example.org"
SCOPES         = ["https://www.googleapis.com/auth/gmail.send"]

print(f"Impersonating: {DELEGATED_USER}")
print(f"Sending to:    {TO}")
print()

# 1. Build credentials impersonating the delegated user, get an access token
try:
    creds = service_account.Credentials.from_service_account_file(
        KEY_FILE, scopes=SCOPES
    ).with_subject(DELEGATED_USER)
    creds.refresh(Request())
    print("✓ Got OAuth2 access token via domain-wide delegation")
except Exception as e:
    print(f"✗ Failed to get token: {e}")
    sys.exit(1)

# 2. Build the message
msg = MIMEText(
    "This is a test email sent via the Gmail API using the service account.\n\n"
    "If you received this, automated Dagster failure alerts are ready to go.\n\n"
    "No action required."
)
msg["to"]      = TO
msg["from"]    = DELEGATED_USER
msg["subject"] = "[Dagster] Gmail API test — alerts working"
raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

# 3. Send via Gmail API REST
resp = requests.post(
    "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
    headers={"Authorization": f"Bearer {creds.token}"},
    json={"raw": raw},
    timeout=15,
)

if resp.status_code == 200:
    print(f"✓ Sent. Message ID: {resp.json().get('id')}")
else:
    print(f"✗ Send failed [{resp.status_code}]: {resp.text}")
    sys.exit(1)
