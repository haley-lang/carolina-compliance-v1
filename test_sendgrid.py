import os
from urllib.error import HTTPError
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

load_dotenv(override=True)

api_key = os.getenv("SENDGRID_API_KEY")
from_email = os.getenv("SENDGRID_FROM_EMAIL")

if not api_key:
    raise RuntimeError("Missing SENDGRID_API_KEY")

if not from_email:
    raise RuntimeError("Missing SENDGRID_FROM_EMAIL")

message = Mail(
    from_email=from_email,
    to_emails=from_email,
    subject="SendGrid test - Carolina Compliance Solutions",
    plain_text_content="This is a controlled test email from SendGrid."
)

try:
    sg = SendGridAPIClient(api_key)
    response = sg.send(message)
    print("STATUS:", response.status_code)
    print("BODY:", response.body)
    print("HEADERS:", response.headers)
except Exception as e:
    print("ERROR:", str(e))
    if isinstance(e, HTTPError):
        print("ERROR STATUS:", e.code)
        print("ERROR BODY:", e.read().decode("utf-8", errors="ignore"))