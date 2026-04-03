import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyairtable import Api
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Cc, Email, Mail


load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDGRID_FROM_EMAIL = os.getenv("SENDGRID_FROM_EMAIL")


def _coerce_client_name(raw_value) -> str:
    if isinstance(raw_value, list):
        return raw_value[0] if raw_value else "Client"
    if isinstance(raw_value, str) and raw_value.strip():
        return raw_value.strip()
    return "Client"


def send_initial_vendor_requests() -> None:
    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, SENDGRID_API_KEY, SENDGRID_FROM_EMAIL]):
        raise RuntimeError(
            "Missing required environment variables: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, "
            "SENDGRID_API_KEY, SENDGRID_FROM_EMAIL"
        )

    api = Api(AIRTABLE_API_KEY)
    email_queue_table = api.table(AIRTABLE_BASE_ID, "Email Queue")
    sendgrid = SendGridAPIClient(api_key=SENDGRID_API_KEY)

    formula = (
        "AND("  # Minimal safe send criteria using current Email Queue schema.
        "{Email Status}='Pending',"
        "OR({Email Type}='Initial Request',{Email Type}='Deficiency Request'),"
        "{Record Status}='Active',"
        "{Primary Email}!=''"
        ")"
    )
    try:
        queue_records = email_queue_table.all(formula=formula)
    except Exception as exc:
        logger.error("Airtable error querying Email Queue: %s", exc)
        return

    total_records = len(queue_records)
    sent_successfully = 0
    failed_sends = 0
    skipped = 0

    logger.info("Email Queue records ready to send: %d", len(queue_records))

    for queue_record in queue_records:
        record_id = queue_record.get("id", "unknown") if isinstance(queue_record, dict) else "unknown"
        fields = queue_record.get("fields", {}) if isinstance(queue_record, dict) else {}
        recipient = fields.get("Primary Email")
        email_type = fields.get("Email Type")
        try:
            cc_raw = fields.get("CC Emails", [])
            cc_values = cc_raw if isinstance(cc_raw, list) else [cc_raw]
            cc_values = [email for email in cc_values if isinstance(email, str) and email.strip()]
            subject = fields.get("Subject", "")
            body = fields.get("Body", "")

            if not recipient:
                skipped += 1
                logger.info(
                    "record_id=%s email_type=%s recipient=%s failure=missing_primary_email",
                    record_id,
                    email_type,
                    recipient,
                )
                continue

            logger.info("record_id=%s email_type=%s recipient=%s", record_id, email_type, recipient)

            message = Mail(
                from_email=Email(email=SENDGRID_FROM_EMAIL, name="Carolina Compliance Solutions"),
                to_emails=recipient,
                subject=subject,
                plain_text_content=body,
            )
            if cc_values:
                message.personalizations[0].add_cc([Cc(email) for email in cc_values])
            message.reply_to = Email("coi@carolinacompliancesolutions.com")

            response = sendgrid.send(message)
            if response.status_code == 202:
                sent_successfully += 1
                logger.info(
                    "record_id=%s email_type=%s recipient=%s success=sent",
                    record_id,
                    email_type,
                    recipient,
                )
                try:
                    email_queue_table.update(
                        record_id,
                        {
                            "Email Status": "Sent",
                            "Sent At": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                except Exception as exc:
                    logger.error(
                        "Airtable update failure record_id=%s recipient=%s error=%s",
                        record_id,
                        recipient,
                        exc,
                    )
            else:
                failed_sends += 1
                error_message = response.body
                if isinstance(error_message, bytes):
                    error_message = error_message.decode("utf-8", errors="replace")
                logger.error(
                    "SendGrid send failure record_id=%s email_type=%s recipient=%s error=%s",
                    record_id,
                    email_type,
                    recipient,
                    error_message,
                )
        except Exception as exc:
            failed_sends += 1
            logger.error(
                "SendGrid send failure record_id=%s email_type=%s recipient=%s error=%s",
                record_id,
                email_type,
                recipient,
                exc,
            )

    logger.info(
        "Email Queue send summary total_records_considered=%d sent_successfully=%d failed_sends=%d skipped=%d",
        total_records,
        sent_successfully,
        failed_sends,
        skipped,
    )


if __name__ == "__main__":
    send_initial_vendor_requests()
