# module_15_email_queue_builder.py

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
from pyairtable import Api
import config as _cfg
from module_12_vendor_reminder_engine import get_vendors_needing_reminders

# Initialize logging
logging.basicConfig(level=logging.INFO)

HARDCODED_EMAIL_SUBJECT = "Insurance Update Required"
HARDCODED_EMAIL_BODY = (
    "Dear Vendor,\n\n"
    "Please provide an updated Certificate of Insurance (COI) at your earliest convenience.\n\n"
    f"Please reply to this email with your updated certificate of insurance attached, or send it to {_cfg.INBOUND_EMAIL}.\n\n"
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

COMPLIANCE_TRIGGER_STATUSES = {"Has Open Items", "Needs Review", "Missing Coverage"}

GENERIC_FAILURE_MESSAGE = (
    "Your certificate of insurance does not meet the coverage requirements on file. "
    "Please submit an updated COI."
)

def fetch_compliance_failure_reasons(api: Api, base_id: str, vendor_name: str, client_name: str,
                                     vendor_record_id: str = "") -> List[str]:
    """Fetch Compliance Failure Reasons from the Insurance Certificates table
    for a given vendor/client combination.

    Returns a list of failure reason strings, or an empty list if none found.
    """
    try:
        certs_table = api.table(base_id, INSURANCE_CERTS_TABLE_ID)
        if vendor_record_id:
            safe_id = vendor_record_id.replace("'", "\\'")
            formula = f"FIND('{safe_id}', ARRAYJOIN({{Vendor Link}}))"
        else:
            safe_vendor = vendor_name.replace("'", "\\'")
            formula = f"{{Named Insured}} = '{safe_vendor}'"
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
        f"{_cfg.INBOUND_EMAIL}\n\n"
        f"Include {client_name} — {vendor_name} in the subject line.\n\n"
        f"Questions? Reply to this email.\n\n"
        f"Carolina Compliance Solutions\n"
        f"{_cfg.INBOUND_EMAIL}"
        f"\n\n---\n"
        f"This assessment is based solely on certificate of insurance documentation "
        f"submitted to Carolina Compliance Solutions. It does not constitute verification "
        f"of actual insurance coverage, policy terms, or carrier obligations. "
        f"Please consult your insurance advisor for coverage determinations."
    )
    return body


def build_expiration_email_body(
    vendor_name: str,
    client_name: str,
    triggering_policies: List[Dict[str, Any]],
) -> str:
    """Build a personalized expiration reminder with actual policy details."""
    from datetime import date

    safe_vendor = vendor_name or "Vendor"
    today = date.today()

    policy_lines = []
    for policy in triggering_policies:
        fields = policy.get("fields", {})
        policy_type = fields.get("Policy Type", "Insurance")
        policy_number = fields.get("Policy Number", "")
        exp_date_str = fields.get("Expiration Date", "")
        exp_status = fields.get("Expiration Status", "")

        # Calculate days until/since expiration
        days_text = ""
        if exp_date_str:
            try:
                exp_date = date.fromisoformat(exp_date_str)
                delta = (exp_date - today).days
                if delta < 0:
                    days_text = f"expired {abs(delta)} day(s) ago"
                elif delta == 0:
                    days_text = "expires today"
                else:
                    days_text = f"{delta} day(s) from today"
            except ValueError:
                days_text = exp_status or ""
        else:
            days_text = exp_status or ""

        number_part = f" (Policy #{policy_number})" if policy_number else ""
        date_part = f" on {exp_date_str}" if exp_date_str else ""
        line = f"- {policy_type}{number_part} — {days_text}{date_part}"
        policy_lines.append(line)

    if not policy_lines:
        policy_lines = ["- One or more policies require renewal"]

    policies_block = "\n".join(policy_lines)

    body = (
        f"Hi {safe_vendor},\n\n"
        f"This is a reminder from Carolina Compliance Solutions regarding insurance "
        f"documentation on file with {client_name}.\n\n"
        f"The following policies require attention:\n"
        f"{policies_block}\n\n"
        f"Please submit a renewed Certificate of Insurance to:\n"
        f"{_cfg.INBOUND_EMAIL}\n\n"
        f"Include {client_name} — {safe_vendor} in the subject line.\n\n"
        f"Questions? Reply to this email.\n\n"
        f"Carolina Compliance Solutions\n"
        f"{_cfg.INBOUND_EMAIL}"
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
    api = Api(api_key)
    return api.table(base_id, "Email Queue")


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
        return False
    # ─────────────────────────────────────────────────────────────────────

    from datetime import timezone as _tz
    send_after = vendor.get("send_after")
    send_after_str = send_after.isoformat() if send_after else datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    record = {
        "Primary Email": primary_email,
        "Subject": subject,
        "Body": body,
        "Email Type": email_type,
        "Reminder Status": "Queued",
        "Record Status": "Active",
        "Send After": send_after_str,
        "Created At": datetime.now().isoformat(),
    }
    # Write vendor as linked record ID, not name string
    if vendor_record_id:
        record["Vendor"] = [vendor_record_id]
    table.create(record, typecast=True)
    logging.info(f"Queue record created for vendor: {vendor_name}")
    return True

def detect_duplicates(vendor_name: str, email_type: str = None, vendor_record_id: str = None) -> bool:
    """Check the Airtable Email Queue for an existing pending email for this vendor.

    Uses the Vendor linked-record display name (not record ID) because
    ARRAYJOIN on linked fields returns display names in Airtable formulas.
    """
    table = connect_to_airtable()
    safe_name = vendor_name.replace("'", "\\'")
    vendor_clause = f"FIND('{safe_name}', ARRAYJOIN({{Vendor}}))"
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
    """Idempotency check: query Email Queue for a matching record that is either
    still Queued or was Sent within the last 7 days.

    Matches on Primary Email + Email Type (sufficient for dedup since email
    uniquely identifies the vendor). Does NOT use ARRAYJOIN on linked record
    fields — Airtable resolves those to display names, not record IDs.

    Returns True if a duplicate exists (email should be suppressed).
    """
    if not recipient_email:
        return False

    table = connect_to_airtable()
    cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ')

    safe_email = recipient_email.replace("'", "\\'")
    safe_type = email_type.replace("'", "\\'") if email_type else ""

    # Suppress if: same email+type and either still Queued, or Sent within last 7 days
    formula = (
        f"AND("
        f"{{Primary Email}} = '{safe_email}', "
        f"{{Email Type}} = '{safe_type}', "
        f"OR("
        f"{{Reminder Status}} = 'Queued', "
        f"AND({{Reminder Status}} = 'Sent', IS_AFTER({{Send After}}, '{cutoff}'))"
        f")"
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
        return False

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
            f"OR({{{FLD_LOG_DECISION}}} = 'Has Open Items', {{{FLD_LOG_DECISION}}} = 'Needs Review', {{{FLD_LOG_DECISION}}} = 'Missing Coverage')"
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
    is Has Open Items, Needs Review, or Missing Coverage."""
    try:
        assignments_table = api.table(base_id, ASSIGNMENTS_TABLE_ID)
        formula = (
            f"AND("
            f"{{{FLD_ASSIGN_ACTIVE}}} = TRUE(), "
            f"OR({{{FLD_ASSIGN_COMP}}} = 'Has Open Items', {{{FLD_ASSIGN_COMP}}} = 'Needs Review', {{{FLD_ASSIGN_COMP}}} = 'Missing Coverage')"
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
        vendor_results = get_vendors_needing_reminders()
    except Exception as e:
        logging.error(f"Error loading vendors: {e}")
        vendor_results = []
    logging.info(f"Vendors loaded: {len(vendor_results)}")

    # Set up Airtable API for fetching failure reasons
    api_key, base_id = load_env_variables()
    api = Api(api_key)

    for vendor, triggering_policies in vendor_results:
        vendor_name = vendor.get("fields", {}).get("Vendor Name") or vendor.get("fields", {}).get("Name") or "Unknown Vendor"
        primary_email = vendor.get("fields", {}).get("Vendor Email") or vendor.get("fields", {}).get("Email", "")
        client_name = vendor.get("fields", {}).get("Client Name", "") or "your client"
        if not primary_email:
            logging.warning(f"Skipping vendor {vendor_name}: missing primary email")
            continue

        # Build subject with soonest expiration days
        from datetime import date as _date
        _today = _date.today()
        min_days = None
        for _pol in triggering_policies:
            _exp_str = _pol.get("fields", {}).get("Expiration Date", "")
            if _exp_str:
                try:
                    _delta = (_date.fromisoformat(_exp_str) - _today).days
                    if min_days is None or _delta < min_days:
                        min_days = _delta
                except ValueError:
                    pass
        if min_days is not None and min_days < 0:
            subject = f"Action Required: {vendor_name} — Policy Expired"
        elif min_days is not None:
            subject = f"Action Required: {vendor_name} — Policy Expiring in {min_days} Days"
        else:
            subject = f"Action Required: {vendor_name} — Policy Expiring"

        body = build_expiration_email_body(vendor_name, client_name, triggering_policies)
        logging.info(f"Vendor {vendor_name}: expiration reminder with {len(triggering_policies)} policy/policies")

        send_after = compute_next_send_window()
        vendor["send_after"] = send_after

        try:
            created = create_email_queue_record(vendor, subject, body, email_type="reminder")
            if created:
                # Write Last Reminder Threshold on each triggering policy so
                # module_12 won't re-trigger at the same threshold level.
                from airtable_constants import P_LAST_REMINDER_THRESHOLD
                policies_table = api.table(base_id, "Insurance Policies")
                for _pol in triggering_policies:
                    _status = _pol.get("fields", {}).get("Expiration Status", "")
                    _threshold = {
                        "Expiring in 90 Days": "90",
                        "Expiring in 60 Days": "60",
                        "Expiring in 30 Days": "30",
                        "Expiring in 7 Days": "7",
                        "Expired": "0",
                    }.get(_status, "")
                    if _threshold:
                        try:
                            policies_table.update(_pol["id"], {P_LAST_REMINDER_THRESHOLD: _threshold})
                        except Exception as exc:
                            logging.warning("Failed to write threshold for policy %s: %s", _pol["id"], exc)
        except Exception as e:
            logging.error(f"Error processing vendor {vendor_name}: {e}")

    # ── Part 2: Compliance-based reminders ────────────────────────────────
    # SUPERSEDED by module_22_followup_ladder. Noncompliance follow-ups now
    # run through the Day 0 → Day 2 → Day 4 → Day 6 ladder, triggered
    # synchronously from module_7b when it writes a noncompliant decision.
    # The code below is the prior open-ended 7-day re-send loop — left
    # commented for reference in case the ladder needs to be reverted.
    # Do NOT re-enable without also gating the ladder trigger in module_7b.
    # ----------------------------------------------------------------------
    # try:
    #     assignments = get_non_compliant_assignments(api, base_id)
    #     vendors_table = api.table(base_id, VENDORS_TABLE_ID)
    #
    #     for assignment in assignments:
    #         fields = assignment.get("fields", {})
    #
    #         # Resolve vendor record to get name and email
    #         vendor_links = fields.get(FLD_ASSIGN_VENDOR, [])
    #         if not vendor_links:
    #             continue
    #         vendor_id = vendor_links[0]
    #         try:
    #             vendor_record = vendors_table.get(vendor_id)
    #         except Exception:
    #             logging.warning("Could not fetch vendor record %s — skipping", vendor_id)
    #             continue
    #
    #         vendor_name = vendor_record.get("fields", {}).get("Vendor Name", "")
    #         vendor_email = vendor_record.get("fields", {}).get("Vendor Email", "")
    #         if not vendor_name or not vendor_email:
    #             logging.warning("Skipping assignment %s: missing vendor name or email", assignment.get("id"))
    #             continue
    #
    #         # Resolve client name for the email body
    #         client_links = fields.get(FLD_ASSIGN_CLIENT, [])
    #         client_name = "your client"
    #         if client_links:
    #             try:
    #                 clients_table = api.table(base_id, CLIENTS_TABLE_ID)
    #                 client_record = clients_table.get(client_links[0])
    #                 client_name = client_record.get("fields", {}).get("Client Name", client_name)
    #             except Exception:
    #                 pass
    #
    #         # Fetch failure reasons from compliance log
    #         client_id = client_links[0] if client_links else ""
    #         failure_reasons = fetch_compliance_log_failure_reasons(api, base_id, vendor_id, client_id)
    #         body = build_deficiency_email_body(vendor_name, client_name, failure_reasons)
    #
    #         subject = f"Documentation Update Needed: {vendor_name} — {client_name}"
    #         email_type = "non-compliance notice"
    #
    #         send_after = compute_next_send_window()
    #
    #         # Build a proper vendor dict so create_email_queue_record works correctly
    #         vendor_dict = {
    #             "id": vendor_id,
    #             "fields": {
    #                 "Vendor Name": vendor_name,
    #                 "Email": vendor_email,
    #             },
    #             "send_after": send_after,
    #         }
    #
    #         # The idempotency check inside create_email_queue_record handles dedup
    #         try:
    #             create_email_queue_record(vendor_dict, subject, body, email_type=email_type)
    #         except Exception as e:
    #             logging.error("Error queuing compliance reminder for %s: %s", vendor_name, e)
    # except Exception as e:
    #     logging.error("Compliance-based reminder block failed: %s", e)

    logging.info("Module complete")

if __name__ == "__main__":
    run()