import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pyairtable import Api
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Email, Mail


load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDGRID_FROM_EMAIL = os.getenv("SENDGRID_FROM_EMAIL")


def _update_status(table, record_id: str, status: str, set_sent_at: bool = False) -> None:
    fields = {"Reminder Status": status}
    if set_sent_at:
        fields["Sent At"] = datetime.now(timezone.utc).isoformat()

    try:
        table.update(record_id, fields)
    except Exception:
        # If "Sent At" does not exist, fall back to status-only update.
        if set_sent_at:
            table.update(record_id, {"Reminder Status": status})
        else:
            raise


def _parse_sent_at(value: str) -> Optional[datetime]:
    if not value:
        return None

    try:
        # Normalize common UTC suffix for fromisoformat compatibility.
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _archive_old_sent_records(table) -> int:
    sent_records = table.all(formula="{Reminder Status} = 'Sent'")
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    archived_count = 0

    for record in sent_records:
        record_id = record["id"]
        fields = record.get("fields", {})
        sent_at = _parse_sent_at(fields.get("Sent At"))

        # Archive only when all conditions are met:
        # - Reminder Status is Sent (from query)
        # - Sent At exists and parses
        # - Sent At is older than 7 days
        if sent_at is None or sent_at >= cutoff:
            continue

        table.update(record_id, {"Record Status": "Archived"})
        archived_count += 1

    return archived_count


def send_reminders() -> None:
    if not SENDGRID_API_KEY or not SENDGRID_FROM_EMAIL:
        raise RuntimeError("Missing required environment variables: SENDGRID_API_KEY and/or SENDGRID_FROM_EMAIL")

    if not all([API_KEY, BASE_ID]):
        logger.error("Missing required environment variables.")
        return

    api = Api(API_KEY)
    table = api.table(BASE_ID, "Email Queue")
    sendgrid = SendGridAPIClient(api_key=SENDGRID_API_KEY)

    queued = table.all(formula="{Reminder Status} = 'Queued'")
    attempted = len(queued)
    sent = 0
    failed = 0

    logger.info("Queued emails found: %d", attempted)

    for record in queued:
        record_id = record["id"]
        fields = record.get("fields", {})
        recipient = fields.get("Primary Email")
        subject = fields.get("Subject")
        body = fields.get("Body")

        if not recipient or not subject or not body:
            failed += 1
            logger.error(
                "Record %s missing one of required fields: Primary Email, Subject, Body",
                record_id,
            )
            _update_status(table, record_id, "Failed")
            continue

        try:
            message = Mail(
                from_email=Email(
                    email=SENDGRID_FROM_EMAIL,
                    name="Carolina Compliance Solutions"
                ),
                to_emails=recipient,
                subject=subject,
                plain_text_content=body,
            )
            message.reply_to = Email("coi@carolinacompliancesolutions.com")
            response = sendgrid.send(message)

            if response.status_code == 202:
                sent += 1
                _update_status(table, record_id, "Sent", set_sent_at=True)
            else:
                failed += 1
                logger.error("Failed to send record %s: %s", record_id, response.body)
                _update_status(table, record_id, "Failed")
        except Exception as exc:
            failed += 1
            logger.error("Failed to send record %s: %s", record_id, exc)
            _update_status(table, record_id, "Failed")

    logger.info("Emails attempted: %d", attempted)
    logger.info("Emails sent: %d", sent)
    logger.info("Failures: %d", failed)

    archived = _archive_old_sent_records(table)
    print(f"Archived records: {archived}")


if __name__ == "__main__":
    send_reminders()