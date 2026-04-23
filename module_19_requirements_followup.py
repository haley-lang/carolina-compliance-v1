# module_19_requirements_followup.py
# Monitors clients whose insurance requirements have not been received
# and queues follow-up alert emails to Haley at Day 3 and Day 7.

import os
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from pyairtable import Api, Table

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Airtable configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
import config as _cfg
HALEY_EMAIL = _cfg.OWNER_EMAIL

from legal_disclaimer import EMAIL_DISCLAIMER

# Table / field IDs
CLIENTS_TABLE_ID = "tbltnBIWke20IEI3K"
REQUIREMENTS_STATUS_FIELD_ID = "flde7mzXePBqKBjGx"
SERVICE_START_DATE_FIELD_ID = "flduyJEcYbWgFqpgg"
EMAIL_QUEUE_TABLE = "Email Queue"
EMAIL_QUEUE_TABLE_ID = "tblCeRCf6RToTFkbL"
EMAIL_QUEUE_CLIENT_LINK_FIELD_ID = "fldbTJdW79X9G3kal"
EMAIL_QUEUE_CREATED_AT_FIELD_ID = "flduah8p5oIxULYHN"


def get_clients(api: Api):
    """Fetch all records from the Clients table."""
    table = api.table(AIRTABLE_BASE_ID, CLIENTS_TABLE_ID)
    try:
        records = table.all()
        logger.info("Loaded %d client records.", len(records))
        return records
    except Exception as e:
        logger.error("Failed to fetch clients: %s", e)
        return []


def days_since_start(client_record) -> int | None:
    """Return the number of days since the client's Service Start Date, or None."""
    start_date_str = client_record["fields"].get("Service Start Date")
    if not start_date_str:
        return None
    try:
        start_date = date.fromisoformat(start_date_str)
        return (date.today() - start_date).days
    except (ValueError, TypeError):
        return None


def queue_alert_email(api: Api, client_name: str, start_date: str, subject: str, body: str):
    """Create an Email Queue record to alert Haley."""
    table = api.table(AIRTABLE_BASE_ID, EMAIL_QUEUE_TABLE)

    # Duplicate check — skip if an identical Queued alert already exists
    safe_subject = subject.replace("'", "\\'")
    formula = (
        f"AND("
        f"{{Primary Email}} = '{HALEY_EMAIL}', "
        f"{{Subject}} = '{safe_subject}', "
        f"{{Reminder Status}} = 'Queued'"
        f")"
    )
    existing = table.all(formula=formula)
    if existing:
        logger.info("Duplicate alert already queued for %s — skipping.", client_name)
        return

    record = {
        "Primary Email": HALEY_EMAIL,
        "Email Type": "Cancellation Alert",
        "Subject": subject,
        "Body": body,
        "Reminder Status": "Queued",
        "Record Status": "Active",
        "Send After": datetime.now().isoformat(),
        "Created At": datetime.now().isoformat(),
    }
    table.create(record)
    logger.info("Queued alert email for client: %s", client_name)


def annual_review_already_queued(api: Api, client_id: str) -> bool:
    """Check if an Annual Requirements Review alert was already queued for this
    client within the last 30 days by querying the Email Queue table directly
    via its table ID, filtering on the Client link field, subject, and Created At.
    """
    table = api.table(AIRTABLE_BASE_ID, EMAIL_QUEUE_TABLE_ID)
    thirty_days_ago = (date.today() - timedelta(days=30)).isoformat()
    formula = (
        f"AND("
        f"FIND('{client_id}', ARRAYJOIN({{Client}})), "
        f"FIND('Annual Requirements Review', {{Subject}}), "
        f"IS_AFTER({{Created At}}, '{thirty_days_ago}')"
        f")"
    )
    try:
        existing = table.all(formula=formula)
        return len(existing) > 0
    except Exception as e:
        logger.error("Error checking annual review duplicates for %s: %s", client_id, e)
        return False


def queue_annual_review_email(api: Api, client_id: str, client_name: str, start_date_str: str):
    """Queue the annual requirements review alert email for a client."""
    table = api.table(AIRTABLE_BASE_ID, EMAIL_QUEUE_TABLE_ID)

    subject = f"📅 Annual Requirements Review: {client_name} — Time to Check In"
    body = (
        f"{client_name} has been a client for approximately one year "
        f"(start date: {start_date_str}).\n\n"
        f"It's time to reach out and confirm their insurance requirements haven't changed.\n"
        f"Common reasons requirements change:\n"
        f"- New types of projects (higher risk work)\n"
        f"- New contract requirements from their GC clients\n"
        f"- Updated company policy\n\n"
        f"Suggested email to send them:\n"
        f"'Hi [contact name], just checking in on your annual compliance review. "
        f"Have your subcontractor insurance requirements changed in the past year? "
        f"Reply with any updates and we'll adjust your account accordingly.'\n\n"
        f"After you follow up, no action needed in Airtable — this alert fires once "
        f"per year automatically."
        f"{EMAIL_DISCLAIMER}"
    )

    record = {
        "Primary Email": HALEY_EMAIL,
        "Email Type": "Cancellation Alert",
        "Subject": subject,
        "Body": body,
        "Client": [client_id],
        "Reminder Status": "Queued",
        "Record Status": "Active",
        "Send After": datetime.now().isoformat(),
        "Created At": datetime.now().isoformat(),
    }
    table.create(record)
    logger.info("Queued annual review alert for client: %s", client_name)


def update_requirements_status(api: Api, client_id: str, new_status: str):
    """Update the Requirements Status field on a client record."""
    table = api.table(AIRTABLE_BASE_ID, CLIENTS_TABLE_ID)
    try:
        table.update(client_id, {"Requirements Status": new_status}, typecast=True)
        logger.info("Updated client %s Requirements Status → %s", client_id, new_status)
    except Exception as e:
        logger.error("Failed to update Requirements Status for %s: %s", client_id, e)


def run():
    logger.info("=== Module 19: Requirements Follow-Up starting ===")

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        logger.error("Airtable API credentials are missing.")
        return
    if not HALEY_EMAIL:
        logger.error("HALEY_EMAIL is not set in .env.")
        return

    api = Api(AIRTABLE_API_KEY)
    clients = get_clients(api)

    for client in clients:
        fields = client["fields"]
        client_id = client["id"]
        client_name = fields.get("Client Name", client_id)
        requirements_status = fields.get("Requirements Status", "")
        start_date_str = fields.get("Service Start Date", "")
        elapsed = days_since_start(client)

        # --- Day 3 alert: Pending — Awaiting Reply ---
        if requirements_status == "Pending — Awaiting Reply":
            if elapsed is not None and elapsed >= 3:
                subject = f"⚠️ Requirements Not Received: {client_name} — Day 3 Follow Up Needed"
                body = (
                    f"Client {client_name} signed up on {start_date_str} and has not "
                    f"yet replied with their insurance requirements. Please follow up "
                    f"before the next pipeline run evaluates their vendors.\n\n"
                    f"Update their Requirements Status to 'Followed Up' in Airtable "
                    f"after you reach out."
                    f"{EMAIL_DISCLAIMER}"
                )
                queue_alert_email(api, client_name, start_date_str, subject, body)
                update_requirements_status(api, client_id, "Followed Up")
            else:
                logger.info(
                    "Client %s is Pending — Awaiting Reply but only %s day(s) since start. Skipping.",
                    client_name, elapsed,
                )

        # --- Day 7 escalation: Followed Up ---
        elif requirements_status == "Followed Up":
            if elapsed is not None and elapsed >= 7:
                subject = f"🚨 Requirements Still Not Received: {client_name} — 7 Days"
                body = (
                    f"Client {client_name} signed up on {start_date_str} and still has not "
                    f"provided their insurance requirements — it has now been {elapsed} days.\n\n"
                    f"This requires immediate attention. Please escalate or contact the client "
                    f"directly before the next pipeline run.\n\n"
                    f"Update their Requirements Status in Airtable after you reach out."
                    f"{EMAIL_DISCLAIMER}"
                )
                queue_alert_email(api, client_name, start_date_str, subject, body)
                update_requirements_status(api, client_id, "Escalated")
            else:
                logger.info(
                    "Client %s was Followed Up but only %s day(s) since start. Skipping escalation.",
                    client_name, elapsed,
                )

        # --- Annual review: Received clients approaching 1-year anniversary ---
        if requirements_status == "Received" and elapsed is not None:
            # Check if today is within 7 days of the 1-year anniversary
            # (i.e. elapsed is between 358 and 372 days, inclusive)
            years_elapsed = elapsed // 365
            if years_elapsed >= 1:
                # Calculate days since the most recent anniversary
                nearest_anniversary_days = years_elapsed * 365
                distance_from_anniversary = abs(elapsed - nearest_anniversary_days)
                if distance_from_anniversary <= 7:
                    if annual_review_already_queued(api, client_id):
                        logger.info(
                            "Client %s annual review alert already queued in last 30 days — skipping.",
                            client_name,
                        )
                    else:
                        queue_annual_review_email(api, client_id, client_name, start_date_str)

    logger.info("=== Module 19: Requirements Follow-Up complete ===")


if __name__ == "__main__":
    run()
