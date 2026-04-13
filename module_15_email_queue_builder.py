# module_15_email_queue_builder.py

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
from pyairtable import Table, Api
from module_12_vendor_reminder_engine import get_vendors_needing_reminders

# Initialize logging
logging.basicConfig(level=logging.INFO)

HARDCODED_EMAIL_SUBJECT = "Insurance Update Required"
HARDCODED_EMAIL_BODY = (
    "Dear Vendor,\n\n"
    "Please provide an updated Certificate of Insurance (COI) at your earliest convenience.\n\n"
    "Please reply to this email with your updated certificate of insurance attached, or send it to coi@carolinacompliancesolutions.com.\n\n"
    "Thank you,\n"
    "Compliance Team"
)

# Insurance Certificates table — used to pull Compliance Failure Reasons
INSURANCE_CERTS_TABLE_ID = "tbl0IH6zQQsXBff3l"
COMPLIANCE_FAILURE_REASONS_FIELD = "fld3NpMELu5PSzoEp"  # Compliance Failure Reasons

# Vendor Client Assignments table — used for compliance-based reminders
ASSIGNMENTS_TABLE_ID = "tblpYKywfs0YHiQ98"
FLD_ASSIGN_VENDOR = "fld7ZjWTE652bGJve"
FLD_ASSIGN_CLIENT = "fldT229klsR59yvqz"
FLD_ASSIGN_ACTIVE = "fld7aAB0tYKJLzrEw"
FLD_ASSIGN_COMP = "fldC8Iqd488Q9Y6nl"

# Compliance Log table — failure reasons written by module_7b
COMPLIANCE_LOG_TABLE_ID = "tblxdt7DT6V3JCQcW"
FLD_LOG_VENDOR_LINK = "fldkq8vZZsN6uBTL0"
FLD_LOG_CLIENT_LINK = "fldbKVYBi20p6wykZ"
FLD_LOG_FAILURE_REASONS = "fldoTDHEEkekT2jAj"
FLD_LOG_DECISION = "fld6Ml1MK3QpmIICK"
FLD_LOG_TIMESTAMP = "fldWzdPiDQOXqlwhg"

# Vendors & Clients tables
VENDORS_TABLE_ID = "tblsOphSd5DKSZEro"
CLIENTS_TABLE_ID = "tbltnBIWke20IEI3K"

COMPLIANCE_TRIGGER_STATUSES = {"Non-Compliant", "Needs Review", "Missing Coverage"}

GENERIC_FAILURE_MESSAGE = (
    "Your certificate of insurance does not meet the coverage requirements on file. "
    "Please submit an updated COI."
)

def fetch_compliance_failure_reasons(api: Api, base_id: str, vendor_name: str, client_name: str) -> List[str]:
    """Fetch Compliance Failure Reasons from the Insurance Certificates table
    for a given vendor/client combination.

    Returns a list of failure reason strings, or an empty list if none found.
    """
    try:
        certs_table = api.table(base_id, INSURANCE_CERTS_TABLE_ID)
        safe_vendor = vendor_name.replace("'", "\\'")
        formula = f"{{Vendor Name}} = '{safe_vendor}'"
        records = certs_table.all(formula=formula)

        for record in records:
            # Match on client name if available, otherwise take the first record
            record_client = record["fields"].get("Client Name", "")
            # If client_name is provided, try to match; accept first match or first record
            if client_name and isinstance(record_client, str) and record_client != client_name:
                continue

            reasons_raw = record["fields"].get("Compliance Failure Reasons", "")
            if not reasons_raw:
                continue

            # Handle both list and string formats
            if isinstance(reasons_raw, list):
                return [r.strip() for r in reasons_raw if r.strip()]
            elif isinstance(reasons_raw, str):
                return [r.strip() for r in reasons_raw.split("\n") if r.strip()]

        # If we found records but none matched the client, try first record as fallback
        if records:
            reasons_raw = records[0]["fields"].get("Compliance Failure Reasons", "")
            if reasons_raw:
                if isinstance(reasons_raw, list):
                    return [r.strip() for r in reasons_raw if r.strip()]
                elif isinstance(reasons_raw, str):
                    return [r.strip() for r in reasons_raw.split("\n") if r.strip()]

        return []
    except Exception as e:
        logging.error(f"Error fetching compliance failure reasons for {vendor_name}: {e}")
        return []


def build_deficiency_email_body(vendor_name: str, client_name: str, failure_reasons: List[str]) -> str:
    """Build a personalized deficiency email body with specific failure reasons.

    If failure_reasons is empty, falls back to a generic message.
    """
    if failure_reasons:
        reasons_block = "\n".join(f"- {reason}" for reason in failure_reasons)
    else:
        reasons_block = f"- {GENERIC_FAILURE_MESSAGE}"

    body = (
        f"Hi {vendor_name},\n\n"
        f"Your submitted Certificate of Insurance on file with {client_name} has been reviewed "
        f"and documentation deficiencies have been identified.\n\n"
        f"The following items require attention:\n"
        f"{reasons_block}\n\n"
        f"Please send an updated Certificate of Insurance addressing these items to:\n"
        f"coi@carolinacompliancesolutions.com\n\n"
        f"Include {client_name} — {vendor_name} in the subject line.\n\n"
        f"Questions? Reply to this email.\n\n"
        f"Carolina Compliance Solutions\n"
        f"coi@carolinacompliancesolutions.com"
        f"\n\n---\n"
        f"This assessment is based solely on certificate of insurance documentation "
        f"submitted to Carolina Compliance Solutions. It does not constitute verification "
        f"of actual insurance coverage, policy terms, or carrier obligations. "
        f"Please consult your insurance advisor for coverage determinations."
    )
    return body


def load_env_variables():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    return api_key, base_id

def connect_to_airtable():
    # Connect to Airtable
    api_key, base_id = load_env_variables()
    return Table(api_key, base_id, "Email Queue")


def connect_api() -> Api:
    api_key, _ = load_env_variables()
    return Api(api_key)

def compute_next_send_window():
    # Compute the next valid send window
    now = datetime.now()
    # Define the preferred send window
    preferred_days = [1, 2, 3]  # Tuesday, Wednesday, Thursday
    preferred_start_hour = 9
    preferred_end_hour = 11

    # Check if current time is within the preferred window
    if now.weekday() in preferred_days and preferred_start_hour <= now.hour < preferred_end_hour:
        return now

    # Calculate the next valid send window
    days_ahead = (preferred_days[0] - now.weekday() + 7) % 7
    if days_ahead == 0 and now.hour >= preferred_end_hour:
        days_ahead = 7  # Move to the next week

    next_send_date = now + timedelta(days=days_ahead)
    next_send_time = next_send_date.replace(hour=preferred_start_hour, minute=0, second=0, microsecond=0)
    return next_send_time

def create_email_queue_record(
    vendor: Dict[str, Any],
    subject: str,
    body: str,
    email_type: str = "non-compliance notice",
    cert_id: str = None,
):
    """Create a record in the Email Queue table with idempotency guard.

    Before inserting, checks for a duplicate record (same recipient, email type,
    vendor/cert ID) created in the last 24 hours.  If a duplicate exists the
    record is silently skipped and the pipeline continues.
    """
    table = connect_to_airtable()
    vendor_name = vendor.get("fields", {}).get("Vendor Name") or vendor.get("fields", {}).get("Name") or "Unknown Vendor"
    primary_email = vendor.get("fields", {}).get("Vendor Email") or vendor.get("fields", {}).get("Email", "")
    vendor_record_id = vendor.get("id", "")

    # ── Idempotency check ────────────────────────────────────────────────
    if check_duplicate_email(
        recipient_email=primary_email,
        email_type=email_type,
        vendor_id=vendor_record_id,
        cert_id=cert_id,
    ):
        # Duplicate exists — skip creation, continue pipeline
        return
    # ─────────────────────────────────────────────────────────────────────

    record = {
        "Vendor": vendor_name,
        "Primary Email": primary_email,
        "CC Emails": [],
        "Subject": subject,
        "Body": body,
        "Email Type": email_type,
        "Reminder Reasons": [],
        "Reminder Status": "Queued",
        "Record Status": "Active",
        "Send After": vendor["send_after"].isoformat(),
        "Follow-Up Count": 0,
        "Created At": datetime.now().isoformat()
    }
    table.create(record)
    logging.info(f"Queue record created for vendor: {vendor_name}")

def detect_duplicates(vendor_name: str, email_type: str = None, vendor_record_id: str = None) -> bool:
    """Check the Airtable Email Queue for an existing pending email for this vendor.

    If vendor_record_id is provided, matches on the linked Vendor record ID.
    Otherwise falls back to matching on vendor name string and logs a warning.
    """
    table = connect_to_airtable()
    if vendor_record_id:
        safe_id = vendor_record_id.replace("'", "\\'")
        vendor_clause = f"FIND('{safe_id}', ARRAYJOIN({{Vendor}}))"
    else:
        logging.warning("detect_duplicates: no vendor_record_id for '%s', falling back to name match", vendor_name)
        safe_name = vendor_name.replace("'", "\\'")
        vendor_clause = f"{{Vendor}} = '{safe_name}'"
    if email_type:
        safe_type = email_type.replace("'", "\\'")
        formula = (
            f"AND("
            f"{vendor_clause}, "
            f"{{Subject}} = '{safe_type}', "
            f"{{Record Status}} = 'Active', "
            f"{{Reminder Status}} = 'Queued'"
            f")"
        )
    else:
        formula = (
            f"AND("
            f"{vendor_clause}, "
            f"{{Record Status}} = 'Active', "
            f"{{Reminder Status}} = 'Queued'"
            f")"
        )
    records = table.all(formula=formula)
    return len(records) > 0


def check_duplicate_email(
    recipient_email: str,
    email_type: str,
    vendor_id: str = None,
    cert_id: str = None,
) -> bool:
    """Idempotency check: query Email Queue for a matching record created within 24 hours.

    Matches on:
      - Same Primary Email (recipient)
      - Same Email Type
      - Same Vendor record ID or Certificate ID (whichever is provided)
      - Created At within the last 24 hours

    Uses a filtered Airtable formula to avoid full table scans.
    Returns True if a duplicate exists (email should be suppressed).
    """
    if not recipient_email:
        return False

    table = connect_to_airtable()
    cutoff = (datetime.now() - timedelta(hours=24)).isoformat()

    safe_email = recipient_email.replace("'", "\\'")
    safe_type = email_type.replace("'", "\\'") if email_type else ""

    # Build the identifier clause — prefer cert_id, fall back to vendor_id
    if cert_id:
        safe_cert = cert_id.replace("'", "\\'")
        id_clause = f"FIND('{safe_cert}', ARRAYJOIN({{Policy}}))"
    elif vendor_id:
        safe_vendor = vendor_id.replace("'", "\\'")
        id_clause = f"FIND('{safe_vendor}', ARRAYJOIN({{Vendor}}))"
    else:
        id_clause = "TRUE()"  # No ID filter available — match on email + type + time only

    formula = (
        f"AND("
        f"{{Primary Email}} = '{safe_email}', "
        f"{{Email Type}} = '{safe_type}', "
        f"{id_clause}, "
        f"IS_AFTER({{Created At}}, '{cutoff}')"
        f")"
    )

    try:
        records = table.all(formula=formula)
        if records:
            identifier = cert_id or vendor_id or "N/A"
            logging.warning(
                "Duplicate email suppressed for %s %s %s",
                recipient_email,
                email_type,
                identifier,
            )
            return True
        return False
    except Exception as e:
        logging.error("Idempotency check failed for %s: %s — allowing email to proceed", recipient_email, e)
        return False  # Fail open: if the check errors, allow the email rather than blocking the pipeline

def fetch_compliance_log_failure_reasons(api: Api, base_id: str, vendor_id: str, client_id: str) -> List[str]:
    """Fetch the most recent failure reasons from the Compliance Log for a vendor/client pair.

    Module 7b writes a log entry every time it evaluates a vendor/client assignment.
    We pull the newest Non-Compliant or Needs Review entry to get the specific reasons.
    """
    try:
        log_table = api.table(base_id, COMPLIANCE_LOG_TABLE_ID)
        safe_vendor = vendor_id.replace("'", "\\'")
        safe_client = client_id.replace("'", "\\'")
        formula = (
            f"AND("
            f"FIND('{safe_vendor}', ARRAYJOIN({{{FLD_LOG_VENDOR_LINK}}})), "
            f"FIND('{safe_client}', ARRAYJOIN({{{FLD_LOG_CLIENT_LINK}}})), "
            f"OR({{{FLD_LOG_DECISION}}} = 'Non-Compliant', {{{FLD_LOG_DECISION}}} = 'Needs Review', {{{FLD_LOG_DECISION}}} = 'Missing Coverage')"
            f")"
        )
        records = log_table.all(formula=formula)
        if not records:
            return []

        # Sort by timestamp descending to get most recent
        records.sort(
            key=lambda r: r["fields"].get(FLD_LOG_TIMESTAMP, ""),
            reverse=True,
        )
        newest = records[0]
        reasons_raw = newest["fields"].get(FLD_LOG_FAILURE_REASONS, "")
        if not reasons_raw:
            return []
        if isinstance(reasons_raw, list):
            return [r.strip() for r in reasons_raw if r.strip()]
        return [r.strip() for r in reasons_raw.split("\n") if r.strip()]
    except Exception as e:
        logging.error(f"Error fetching compliance log reasons for vendor {vendor_id}: {e}")
        return []


def get_non_compliant_assignments(api: Api, base_id: str) -> List[Dict[str, Any]]:
    """Query Vendor Client Assignments for active records where Compliance Status
    is Non-Compliant or Needs Review."""
    try:
        assignments_table = api.table(base_id, ASSIGNMENTS_TABLE_ID)
        formula = (
            f"AND("
            f"{{{FLD_ASSIGN_ACTIVE}}} = TRUE(), "
            f"OR({{{FLD_ASSIGN_COMP}}} = 'Non-Compliant', {{{FLD_ASSIGN_COMP}}} = 'Needs Review', {{{FLD_ASSIGN_COMP}}} = 'Missing Coverage')"
            f")"
        )
        records = assignments_table.all(formula=formula)
        logging.info(f"Non-compliant/needs-review assignments found: {len(records)}")
        return records
    except Exception as e:
        logging.error(f"Error fetching non-compliant assignments: {e}")
        return []


def run():
    logging.info("Module start")

    # ── Part 1: Expiration-based reminders ───────────────────────────────────
    try:
        vendors = get_vendors_needing_reminders()
    except Exception as e:
        logging.error(f"Error loading vendors: {e}")
        vendors = []
    logging.info(f"Vendors loaded: {len(vendors)}")

    # Set up Airtable API for fetching failure reasons
    api_key, base_id = load_env_variables()
    api = Api(api_key)

    for vendor in vendors:
        vendor_name = vendor.get("fields", {}).get("Vendor Name") or vendor.get("fields", {}).get("Name") or "Unknown Vendor"
        primary_email = vendor.get("fields", {}).get("Email", "")
        client_name = vendor.get("fields", {}).get("Client Name", "") or "your client"
        if not primary_email:
            logging.warning(f"Skipping vendor {vendor_name}: missing primary email")
            continue

        subject = HARDCODED_EMAIL_SUBJECT

        # Fetch specific failure reasons from Insurance Certificates table
        failure_reasons = fetch_compliance_failure_reasons(api, base_id, vendor_name, client_name)
        body = build_deficiency_email_body(vendor_name, client_name, failure_reasons)

        if failure_reasons:
            logging.info(f"Vendor {vendor_name}: {len(failure_reasons)} failure reason(s) pulled from Airtable")
        else:
            logging.info(f"Vendor {vendor_name}: no failure reasons found, using generic message")

        send_after = compute_next_send_window()
        vendor["send_after"] = send_after

        try:
            # The idempotency check inside create_email_queue_record handles dedup
            create_email_queue_record(vendor, subject, body, email_type="reminder")
        except Exception as e:
            logging.error(f"Error processing vendor {vendor_name}: {e}")

    # ── Part 2: Compliance-based reminders (Non-Compliant / Needs Review from module_7b)
    try:
        assignments = get_non_compliant_assignments(api, base_id)
        vendors_table = api.table(base_id, VENDORS_TABLE_ID)

        for assignment in assignments:
            fields = assignment.get("fields", {})

            # Resolve vendor record to get name and email
            vendor_links = fields.get(FLD_ASSIGN_VENDOR, [])
            if not vendor_links:
                continue
            vendor_id = vendor_links[0]
            try:
                vendor_record = vendors_table.get(vendor_id)
            except Exception:
                logging.warning("Could not fetch vendor record %s — skipping", vendor_id)
                continue

            vendor_name = vendor_record.get("fields", {}).get("Vendor Name", "")
            vendor_email = vendor_record.get("fields", {}).get("Vendor Email", "")
            if not vendor_name or not vendor_email:
                logging.warning("Skipping assignment %s: missing vendor name or email", assignment.get("id"))
                continue

            # Resolve client name for the email body
            client_links = fields.get(FLD_ASSIGN_CLIENT, [])
            client_name = "your client"
            if client_links:
                try:
                    clients_table = api.table(base_id, CLIENTS_TABLE_ID)
                    client_record = clients_table.get(client_links[0])
                    client_name = client_record.get("fields", {}).get("Client Name", client_name)
                except Exception:
                    pass

            # Fetch failure reasons from compliance log
            client_id = client_links[0] if client_links else ""
            failure_reasons = fetch_compliance_log_failure_reasons(api, base_id, vendor_id, client_id)
            body = build_deficiency_email_body(vendor_name, client_name, failure_reasons)

            subject = HARDCODED_EMAIL_SUBJECT
            email_type = "non-compliance notice"

            send_after = compute_next_send_window()

            # Build a proper vendor dict so create_email_queue_record works correctly
            vendor_dict = {
                "id": vendor_id,
                "fields": {
                    "Vendor Name": vendor_name,
                    "Email": vendor_email,
                },
                "send_after": send_after,
            }

            # The idempotency check inside create_email_queue_record handles dedup
            try:
                create_email_queue_record(vendor_dict, subject, body, email_type=email_type)
            except Exception as e:
                logging.error("Error queuing compliance reminder for %s: %s", vendor_name, e)
    except Exception as e:
        logging.error("Compliance-based reminder block failed: %s", e)

    logging.info("Module complete")

if __name__ == "__main__":
    run()