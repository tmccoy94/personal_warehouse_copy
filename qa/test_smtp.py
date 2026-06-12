"""Quick SMTP test — sends a test email using the configured credentials."""
import os, smtplib, sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

smtp_host = os.environ.get("SMTP_HOST", "smtp-relay.gmail.com")
smtp_port = int(os.environ.get("SMTP_PORT", "587"))
smtp_user = os.environ.get("SMTP_USER", "")
smtp_pass = os.environ.get("SMTP_PASSWORD", "")
from_addr = os.environ.get("SMTP_FROM", smtp_user)
to_addr   = os.environ.get("ALERT_EMAIL", "")

print(f"SMTP host : {smtp_host}:{smtp_port}")
print(f"From      : {from_addr}")
print(f"To        : {to_addr}")
print(f"Auth user : {smtp_user}")
print()

if not to_addr:
    print("ERROR: ALERT_EMAIL not set in environment")
    sys.exit(1)

msg = MIMEMultipart()
msg["Subject"] = "[Dagster] SMTP test — if you see this, email alerts are working"
msg["From"]    = from_addr
msg["To"]      = to_addr
msg.attach(MIMEText(
    "This is a test email from the Dagster pipeline notification system.\n\n"
    "If you received this, SMTP is configured correctly and job failure\n"
    "alerts will reach you when scheduled runs fail.\n\n"
    "No action required.",
    "plain"
))

try:
    print("Connecting...")
    with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
        server.set_debuglevel(1)
        server.ehlo()
        server.starttls()
        server.ehlo()
        if smtp_user and smtp_pass:
            print("Logging in...")
            server.login(smtp_user, smtp_pass)
        print("Sending...")
        server.sendmail(from_addr, [to_addr], msg.as_string())
    print(f"\n✓ Test email sent to {to_addr}")
except Exception as e:
    print(f"\n✗ Failed: {e}")
    sys.exit(1)
