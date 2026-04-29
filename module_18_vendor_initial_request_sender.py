import logging
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from pyairtable import Api
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Cc, Email, Mail


load_dotenv(override=True)

import config as _cfg
from airtable_constants import (
    TBL_EMAIL_QUEUE,
    EQ_PRIMARY_EMAIL, EQ_SUBJECT, EQ_BODY, EQ_CC_EMAILS,
    EQ_EMAIL_TYPE, EQ_EMAIL_STATUS, EQ_RECORD_STATUS, EQ_SENT_AT,
    EQ_VENDOR_LINK, EQ_CLIENT_LINK,
    C_REQUIREMENTS_STATUS,
    EMAIL_TYPE_INITIAL_REQUEST, EMAIL_TYPE_DEFICIENCY_REQUEST,
    STATUS_PENDING, STATUS_ACTIVE, STATUS_SENT,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDGRID_FROM_EMAIL = os.getenv("SENDGRID_FROM_EMAIL")

# --- Airtable table / field IDs for guard-clause and dedup lookups ---
CLIENTS_TABLE_ID = "tbltnBIWke20IEI3K"
FLD_REQUIREMENTS_STATUS = "flde7mzXePBqKBjGx"
EMAIL_QUEUE_TABLE_ID = "tblCeRCf6RToTFkbL"
FLD_EQ_VENDOR_LINK = "fldvEsppIZASymJjF"
FLD_EQ_EMAIL_TYPE = "fldtVfbc7XNSVG1pT"
FLD_EQ_CREATED_AT = "flduah8p5oIxULYHN"


def _client_requirements_received(api: Api, client_record_id: str) -> bool:
    """Return True only if the vendor's linked client has Requirements Status = 'Received'."""
    if not client_record_id:
        return False
    try:
        clients_table = api.table(AIRTABLE_BASE_ID, CLIENTS_TABLE_ID)
        record = clients_table.get(client_record_id)
        status = record.get("fields", {}).get(C_REQUIREMENTS_STATUS, "")
        return status == "Received"
    except Exception as exc:
        logger.error("Failed to check client requirements status: %s", exc)
        return False


def _initial_request_sent_recently(api: Api, vendor_record_id: str) -> bool:
    """Return True if an 'Initial Request' email was sent to this vendor in the last 30 days."""
    if not vendor_record_id:
        return False
    try:
        eq_table = api.table(AIRTABLE_BASE_ID, EMAIL_QUEUE_TABLE_ID)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        formula = (
            f"AND("
            f"FIND(\"{vendor_record_id}\", ARRAYJOIN({{{FLD_EQ_VENDOR_LINK}}})),"
            f"{{{FLD_EQ_EMAIL_TYPE}}}='Initial Request',"
            f"IS_AFTER({{{FLD_EQ_CREATED_AT}}}, '{cutoff}')"
            f")"
        )
        records = eq_table.all(formula=formula)
        return len(records) > 0
    except Exception as exc:
        logger.error("Failed to check duplicate send for vendor %s: %s", vendor_record_id, exc)
        return False


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
    email_queue_table = api.table(AIRTABLE_BASE_ID, TBL_EMAIL_QUEUE)
    sendgrid = SendGridAPIClient(api_key=SENDGRID_API_KEY)

    formula = (
        "AND("  # Minimal safe send criteria using current Email Queue schema.
        f"{{{EQ_EMAIL_STATUS}}}='{STATUS_PENDING}',"
        f"OR({{{EQ_EMAIL_TYPE}}}='{EMAIL_TYPE_INITIAL_REQUEST}',{{{EQ_EMAIL_TYPE}}}='{EMAIL_TYPE_DEFICIENCY_REQUEST}'),"
        f"{{{EQ_RECORD_STATUS}}}='{STATUS_ACTIVE}',"
        f"{{{EQ_PRIMARY_EMAIL}}}!=''"
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
        recipient = fields.get(EQ_PRIMARY_EMAIL)
        email_type = fields.get(EQ_EMAIL_TYPE)
        vendor_name = fields.get("Vendor Name", "Vendor")
        if isinstance(vendor_name, list):
            vendor_name = vendor_name[0] if vendor_name else "Vendor"
        gc_client_name = _coerce_client_name(fields.get("Client Name"))

        # --- Guard clause: linked vendor record ID and client record ID ---
        # Use airtable_constants for field names. The Email Queue schema
        # names these "Vendor" and "Client" (EQ_VENDOR_LINK / EQ_CLIENT_LINK),
        # not "Vendor Link" / "Client Link" — that mismatch silently broke
        # both the requirements-confirmed and duplicate-send guards.
        vendor_link_raw = fields.get(EQ_VENDOR_LINK, [])
        vendor_record_id = vendor_link_raw[0] if isinstance(vendor_link_raw, list) and vendor_link_raw else None
        client_link_raw = fields.get(EQ_CLIENT_LINK, [])
        client_record_id = client_link_raw[0] if isinstance(client_link_raw, list) and client_link_raw else None

        # --- GUARD: Client requirements must be confirmed ---
        if email_type == EMAIL_TYPE_INITIAL_REQUEST and not _client_requirements_received(api, client_record_id):
            skipped += 1
            logger.info("Skipping %s — client requirements not yet confirmed", vendor_name)
            continue

        # --- GUARD: Duplicate send prevention (Initial Request within 30 days) ---
        if email_type == EMAIL_TYPE_INITIAL_REQUEST and _initial_request_sent_recently(api, vendor_record_id):
            skipped += 1
            logger.info("Skipping %s — initial request already sent within 30 days", vendor_name)
            continue

        try:
            cc_raw = fields.get(EQ_CC_EMAILS, [])
            cc_values = cc_raw if isinstance(cc_raw, list) else [cc_raw]
            cc_values = [email for email in cc_values if isinstance(email, str) and email.strip()]

            # --- Build subject and body ---
            if email_type == EMAIL_TYPE_INITIAL_REQUEST:
                subject = f"{gc_client_name} — Certificate of Insurance Request"
                body = (
                    f"Hi {vendor_name},\n\n"
                    f"{gc_client_name} has asked us to collect a current Certificate "
                    f"of Insurance for your company. They use Carolina Compliance "
                    f"Solutions to manage subcontractor insurance compliance.\n\n"
                    f"Please email your COI to:\n"
                    f"{_cfg.INBOUND_EMAIL}\n\n"
                    f"In the subject line, please include:\n"
                    f"{gc_client_name} — {vendor_name}\n\n"
                    f"Once we receive it, our system will process it automatically.\n\n"
                    f"If anything is missing or doesn't meet requirements, we'll\n"
                    f"follow up with you directly.\n\n"
                    f"When your policy renews, just send the updated certificate to\n"
                    f"the same address — no login or portal required.\n\n"
                    f"Questions? Reply to this email.\n\n"
                    f"Carolina Compliance Solutions\n"
                    f"{_cfg.INBOUND_EMAIL}"
                )
            else:
                subject = fields.get(EQ_SUBJECT, "")
                body = fields.get(EQ_BODY, "")

            # Append disclaimer to all email bodies
            from legal_disclaimer import EMAIL_DISCLAIMER
            body = body + EMAIL_DISCLAIMER if body else body

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
            message.reply_to = Email(_cfg.INBOUND_EMAIL)

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
                            EQ_EMAIL_STATUS: STATUS_SENT,
                            EQ_SENT_AT: datetime.now(timezone.utc).isoformat(),
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
