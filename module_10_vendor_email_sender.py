import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from pyairtable import Api

# Load environment variables from .env file
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Read Airtable configuration from environment variables
API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Cc, Content
from email_template import build_email_html
import config as _cfg
from airtable_constants import (
    TBL_EMAIL_QUEUE, EQ_PRIMARY_EMAIL, EQ_SUBJECT, EQ_BODY, EQ_CC_EMAILS,
    EQ_REMINDER_STATUS, EQ_SEND_AFTER, STATUS_QUEUED, STATUS_SENT, STATUS_FAILED,
)

# Email Queue Email Type values that belong to a follow-up ladder. Module_10
# reads Email Type + Reminder Reason ("LADDER:<record_id>") after a successful
# send and calls module_22.record_send_success to stamp Day N Sent At.
LADDER_EQ_TYPES = {
    "Ladder Day 0", "Ladder Day 0 Retry",
    "Ladder Day 2", "Ladder Day 4",
    "Ladder Day 6 Approval Request",
}

# Audience routing for Reply-To. Drives config.reply_to_for(audience):
#   vendor   -> INBOUND_EMAIL (replies feed the IMAP poller)
#   client   -> OWNER_EMAIL   (Haley reads + responds personally)
#   internal -> OWNER_EMAIL   (system→owner notifications)
# Unknown / unmapped Email Type defaults to "internal" (safest — lands with Haley).
EMAIL_TYPE_AUDIENCE = {
    "Initial Request": "vendor",
    "Deficiency Request": "vendor",
    "reminder": "vendor",
    "Ladder Day 0": "vendor",
    "Ladder Day 0 Retry": "vendor",
    "Ladder Day 2": "vendor",
    "Ladder Day 4": "vendor",
    "Ladder Day 6 Approval Request": "internal",
    "Cancellation Alert": "client",
    "Reinstatement Request": "vendor",
    "Reinstatement Alert": "internal",
    "Endorsement Alert": "internal",
    "Bounce Alert": "internal",
    "No Attachment Alert": "internal",
    "Cloud Link Alert": "internal",
    "Escalation Alert": "internal",
    "Exception Alert": "client",
}

def send_queued_emails(api):
    sg = SendGridAPIClient(api_key=os.getenv("SENDGRID_API_KEY"))
    from_email = Email(os.getenv("SENDGRID_FROM_EMAIL"), "Carolina Compliance Solutions")
    table = api.table(BASE_ID, TBL_EMAIL_QUEUE)
    records = table.all(formula=f"AND({{{EQ_REMINDER_STATUS}}} = '{STATUS_QUEUED}', {{{EQ_SEND_AFTER}}} <= NOW())")
    record_count = len(records)
    logging.info(f"Found {record_count} queued email record(s).")
    if record_count == 0:
        logging.info("No queued email records found.")
        
    for record in records:
        fields = record['fields']
        missing_fields = [field for field in [EQ_PRIMARY_EMAIL, EQ_SUBJECT, EQ_BODY] if field not in fields]
        if missing_fields:
            logging.error(f"Record ID {record['id']} is missing required fields: {', '.join(missing_fields)}")
            continue
        try:
            to_email = To(fields[EQ_PRIMARY_EMAIL])
            cc_emails = [Cc(email) for email in fields.get(EQ_CC_EMAILS, [])]
            subject = fields[EQ_SUBJECT]
            raw_body = fields[EQ_BODY]

            # Audience drives both Reply-To and the footer email rendered by
            # build_email_html. Default to "internal" for unknown Email Types
            # — safest landing zone (Haley) rather than the automated COI inbox.
            audience = EMAIL_TYPE_AUDIENCE.get(fields.get("Email Type"), "internal")

            # If the body is already a full HTML email (from module_8b etc.),
            # send as-is; otherwise wrap plain text in the branded template.
            if raw_body.strip().startswith("<!DOCTYPE") or raw_body.strip().startswith("<html"):
                html_body = raw_body
            else:
                body_html = "<p>" + raw_body.replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>"
                html_body = build_email_html(subject, body_html, audience=audience)

            mail = Mail(
                from_email=from_email,
                to_emails=to_email,
                subject=subject,
            )
            mail.add_content(Content("text/html", html_body))
            mail.reply_to = Email(_cfg.reply_to_for(audience))
            if cc_emails:
                mail.personalizations[0].add_cc(cc_emails)

            response = sg.send(mail)
            if response.status_code == 202:
                update_email_status(api, record['id'], STATUS_SENT)
                _maybe_stamp_ladder(api, fields)
            else:
                logging.error(f"Failed to send email to {fields[EQ_PRIMARY_EMAIL]}: {response.body}")
                update_email_status(api, record['id'], STATUS_FAILED)
        except Exception as e:
            logging.error(f"Failed to send email to {fields[EQ_PRIMARY_EMAIL]}: {e}")
            update_email_status(api, record['id'], STATUS_FAILED)


def _maybe_stamp_ladder(api, fields):
    """If this Email Queue row belongs to a follow-up ladder, stamp the
    Day N Sent At field on the ladder record. Never raises."""
    eq_type = fields.get("Email Type") or ""
    if eq_type not in LADDER_EQ_TYPES:
        return
    reason = fields.get("Reminder Reason") or ""
    if not reason.startswith("LADDER:"):
        return
    ladder_record_id = reason[len("LADDER:"):].strip()
    if not ladder_record_id:
        return
    try:
        from module_22_followup_ladder import record_send_success
        record_send_success(ladder_record_id, eq_type, api=api)
    except Exception as exc:
        logging.warning("Failed to stamp ladder %s after send: %s", ladder_record_id, exc)

def update_email_status(api, record_id, status):
    table = api.table(BASE_ID, TBL_EMAIL_QUEUE)
    table.update(record_id, {EQ_REMINDER_STATUS: status})

def run():
    if not API_KEY or not BASE_ID:
        logging.error("Airtable API key or Base ID is missing.")
        return

    api = Api(API_KEY)
    send_queued_emails(api)

if __name__ == "__main__":
    run()