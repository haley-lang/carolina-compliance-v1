import os
from urllib.error import HTTPError

import pytest
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sendgrid_utils import apply_sandbox_if_enabled, sandbox_active

load_dotenv(override=True)

api_key = os.getenv("SENDGRID_API_KEY")
from_email = os.getenv("SENDGRID_FROM_EMAIL")

if not api_key:
    pytest.skip("SENDGRID_API_KEY not set — skipping live SendGrid test", allow_module_level=True)

if not from_email:
    pytest.skip("SENDGRID_FROM_EMAIL not set — skipping live SendGrid test", allow_module_level=True)

is_sandbox = sandbox_active()
print(f"SANDBOX MODE: {is_sandbox}")

message = Mail(
    from_email=from_email,
    to_emails=from_email,
    subject="SendGrid test - Carolina Compliance Solutions",
    plain_text_content="This is a controlled test email from SendGrid."
)

apply_sandbox_if_enabled(message)

try:
    sg = SendGridAPIClient(api_key)
    response = sg.send(message)
    print("STATUS:", response.status_code)
    # 202 = real send accepted; 200 = sandbox accepted (no delivery)
    if is_sandbox:
        print("RESULT: sandbox accepted — no email delivered (expected)")
    else:
        print("RESULT: email submitted for delivery")
    print("BODY:", response.body)
except Exception as e:
    print("ERROR:", str(e))
    if isinstance(e, HTTPError):
        print("ERROR STATUS:", e.code)
        print("ERROR BODY:", e.read().decode("utf-8", errors="ignore"))
