"""
Exception Manager — Carolina Compliance Solutions

Manages GC-approved documentation exceptions:
- Lookup active exceptions during compliance evaluation
- Track exception expiry and queue alerts
- Record human overrides to compliance log
- Queue deficiency alerts when no exception is on file

IMPORTANT: Exceptions are approvals made by the GC, not by this system.
The system surfaces deficiencies, records the GC's decision, and tracks expiry.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from legal_disclaimer import EMAIL_DISCLAIMER

logger = logging.getLogger(__name__)

# ── Airtable table / field IDs ───────────────────────────────────────────────

TABLE_OVERRIDES = "tblKVIdTYwF1Sp5Iw"

# Vendor Requirement Overrides fields
FLD_OVR_VENDOR_LINK      = "fldTcCB7NS5CARill"
FLD_OVR_CLIENT_LINK      = "fldbaM8WduvJyntdY"
FLD_OVR_POLICY_TYPE      = "fldYqRsh1IMbmZ0LS"
FLD_OVR_DEFICIENCY_CODE  = "fld8epEkEouhLnjgO"
FLD_OVR_APPROVED_BY      = "fldMxeHJ9xFepqk9Z"
FLD_OVR_APPROVAL_DATE    = "fldTOJrMYRtEkcotB"
FLD_OVR_EXPIRATION_DATE  = "fldJnuDX4Yz4PBrHF"
FLD_OVR_ACTIVE           = "fldOlypHmwULhX7g1"
FLD_OVR_EXCEPTION_STATUS = "fldo2eXtq8uqp7j1y"
FLD_OVR_REASON           = "fldmUYgGheNvGH9Pk"
FLD_OVR_SUMMARY          = "fld4Jum7fiVYYeuLe"

# Compliance Log fields
FLD_LOG_ENTRY            = "fldzUTll7izbXfBEB"
FLD_LOG_DECISION         = "fld6Ml1MK3QpmIICK"
FLD_LOG_FAILURE_REASONS  = "fldoTDHEEkekT2jAj"
FLD_LOG_VENDOR_LINK      = "fldkq8vZZsN6uBTL0"
FLD_LOG_CLIENT_LINK      = "fldbKVYBi20p6wykZ"
FLD_LOG_TIMESTAMP        = "fldWzdPiDQOXqlwhg"

# Email Queue fields
FLD_EQ_PRIMARY_EMAIL     = "fldWz9aK1If5Z7g3S"
FLD_EQ_SUBJECT           = "fld6v3zOlaNcvcqch"
FLD_EQ_BODY              = "fldvZ7SpNXCoheLpp"
FLD_EQ_EMAIL_TYPE        = "fldtVfbc7XNSVG1pT"
FLD_EQ_REMINDER_STATUS   = "fldzgNFcN8GtcPXt0"
FLD_EQ_SEND_AFTER        = "fldFkI7vgU9PGl92v"
FLD_EQ_VENDOR            = "fldvEsppIZASymJjF"
FLD_EQ_CLIENT            = "fldbTJdW79X9G3kal"

EXCEPTION_EXPIRY_WARNING_DAYS = 7


# ── Date helpers ─────────────────────────────────────────────────────────────

def _parse_date(raw) -> Optional[date]:
    if isinstance(raw, date):
        return raw
    cleaned = (str(raw) if raw else "").strip()
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# ── Exception lookup (called by module_7b) ───────────────────────────────────

def find_active_exception(
    overrides_table,
    vendor_id: str,
    client_id: str,
    policy_type: str,
    deficiency_code: str = "",
) -> Optional[Dict[str, Any]]:
    """Find an active, non-expired exception for a vendor-client-policy line.

    Returns the override record dict if found, None otherwise.
    Filters by vendor ID AND client ID (per-project scoping).
    """
    today = date.today()

    try:
        records = overrides_table.all()
    except Exception as exc:
        logger.error("Failed to fetch overrides: %s", exc)
        return None

    for record in records:
        fields = record.get("fields", {})

        # Must be active
        if not fields.get(FLD_OVR_ACTIVE) and not fields.get("Active"):
            continue

        # Must match vendor
        vendor_links = fields.get(FLD_OVR_VENDOR_LINK) or fields.get("Vendor Link") or []
        if vendor_id not in vendor_links:
            continue

        # Must match client (per-project scoping)
        client_links = fields.get(FLD_OVR_CLIENT_LINK) or fields.get("Client Link") or []
        if client_id not in client_links:
            continue

        # Must match policy type
        record_type = fields.get(FLD_OVR_POLICY_TYPE) or fields.get("Policy Type") or ""
        if isinstance(record_type, dict):
            record_type = record_type.get("name", "")
        if record_type != policy_type:
            continue

        # Must not be expired
        exp_raw = fields.get(FLD_OVR_EXPIRATION_DATE) or fields.get("Expiration Date") or ""
        exp_date = _parse_date(exp_raw)
        if exp_date and exp_date < today:
            continue

        # Optional: match specific deficiency code
        if deficiency_code:
            record_code = fields.get(FLD_OVR_DEFICIENCY_CODE) or fields.get("Deficiency Code") or ""
            if record_code and record_code != deficiency_code:
                continue

        return record

    return None


def format_exception_note(override_record: Dict[str, Any]) -> str:
    """Format a human-readable note about an active exception for the compliance summary."""
    fields = override_record.get("fields", {})
    approved_by = fields.get(FLD_OVR_APPROVED_BY) or fields.get("Approved By") or "authorized reviewer"
    approval_date = fields.get(FLD_OVR_APPROVAL_DATE) or fields.get("Approval Date") or "date not recorded"
    expiry = fields.get(FLD_OVR_EXPIRATION_DATE) or fields.get("Expiration Date") or "no expiration set"
    reason = fields.get(FLD_OVR_REASON) or fields.get("Override Reason") or ""
    policy_type = fields.get(FLD_OVR_POLICY_TYPE) or fields.get("Policy Type") or ""

    note = (
        f"Documentation deficiency noted on {policy_type}. "
        f"Exception approved by {approved_by} on {approval_date}, "
        f"valid through {expiry}."
    )
    if reason:
        note += f" Reason: {reason}"

    return note


# ── Exception expiry checker (called by pipeline cron) ───────────────────────

def check_expiring_exceptions(overrides_table, email_queue_table, clients_table=None):
    """Check for exceptions expiring within 7 days and queue alert emails.

    Also marks expired exceptions as inactive.
    """
    today = date.today()
    warning_cutoff = today + timedelta(days=EXCEPTION_EXPIRY_WARNING_DAYS)

    try:
        records = overrides_table.all()
    except Exception as exc:
        logger.error("Failed to fetch overrides for expiry check: %s", exc)
        return

    expired_count = 0
    expiring_count = 0

    for record in records:
        record_id = record["id"]
        fields = record.get("fields", {})

        is_active = fields.get(FLD_OVR_ACTIVE) or fields.get("Active")
        if not is_active:
            continue

        exp_raw = fields.get(FLD_OVR_EXPIRATION_DATE) or fields.get("Expiration Date") or ""
        exp_date = _parse_date(exp_raw)
        if not exp_date:
            continue

        vendor_name = ""
        vendor_links = fields.get(FLD_OVR_VENDOR_LINK) or fields.get("Vendor Link") or []
        client_links = fields.get(FLD_OVR_CLIENT_LINK) or fields.get("Client Link") or []
        policy_type = fields.get(FLD_OVR_POLICY_TYPE) or fields.get("Policy Type") or ""
        if isinstance(policy_type, dict):
            policy_type = policy_type.get("name", "")

        # Expired — mark inactive
        if exp_date < today:
            try:
                overrides_table.update(record_id, {
                    FLD_OVR_ACTIVE: False,
                    FLD_OVR_EXCEPTION_STATUS: "Expired",
                }, typecast=True)
                logger.info("Exception %s expired on %s — marked inactive", record_id, exp_date)
                expired_count += 1
            except Exception as exc:
                logger.error("Failed to expire exception %s: %s", record_id, exc)
            continue

        # Expiring soon — queue alert if not already marked
        current_status = fields.get(FLD_OVR_EXCEPTION_STATUS) or fields.get("Exception Status") or ""
        if isinstance(current_status, dict):
            current_status = current_status.get("name", "")

        if exp_date <= warning_cutoff and current_status != "Expiring Soon":
            try:
                overrides_table.update(record_id, {
                    FLD_OVR_EXCEPTION_STATUS: "Expiring Soon",
                }, typecast=True)
            except Exception as exc:
                logger.error("Failed to update exception status %s: %s", record_id, exc)

            # Exception system disabled — post-launch feature, too complex for early clients
            # gc_email = _resolve_gc_email(clients_table, client_links)
            # if gc_email:
            #     days_left = (exp_date - today).days
            #     _queue_exception_expiry_alert(
            #         email_queue_table,
            #         gc_email=gc_email,
            #         vendor_links=vendor_links,
            #         client_links=client_links,
            #         policy_type=policy_type,
            #         expiry_date=exp_date.isoformat(),
            #         days_left=days_left,
            #     )
            #     expiring_count += 1
            pass

    logger.info("Exception expiry check: %d expired, %d expiring soon", expired_count, expiring_count)


def _resolve_gc_email(clients_table, client_links: list) -> str:
    if not clients_table or not client_links:
        return ""
    try:
        client = clients_table.get(client_links[0])
        return client.get("fields", {}).get("Portal Email") or client.get("fields", {}).get("Client Email") or ""
    except Exception:
        return ""


def _queue_exception_expiry_alert(
    email_queue_table, gc_email, vendor_links, client_links,
    policy_type, expiry_date, days_left,
):
    fields = {
        FLD_EQ_PRIMARY_EMAIL: gc_email,
        FLD_EQ_SUBJECT: f"Exception Expiring: {policy_type} documentation exception expires in {days_left} days",
        FLD_EQ_BODY: (
            f"An approved documentation exception is expiring soon.\n\n"
            f"Policy line: {policy_type}\n"
            f"Exception expires: {expiry_date}\n"
            f"Days remaining: {days_left}\n\n"
            f"Please review whether an updated certificate has been received "
            f"or whether this exception should be renewed.\n\n"
            f"If the subcontractor has submitted updated documentation, no action is needed — "
            f"the system will re-evaluate compliance automatically.\n\n"
            f"Carolina Compliance Solutions"
            f"{EMAIL_DISCLAIMER}"
        ),
        FLD_EQ_EMAIL_TYPE: "Exception Alert",
        FLD_EQ_REMINDER_STATUS: "Queued",
        FLD_EQ_SEND_AFTER: datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    if vendor_links:
        fields[FLD_EQ_VENDOR] = vendor_links[:1]
    if client_links:
        fields[FLD_EQ_CLIENT] = client_links[:1]

    try:
        email_queue_table.create(fields, typecast=True)
        logger.info("Exception expiry alert queued for %s", gc_email)
    except Exception as exc:
        logger.error("Failed to queue exception expiry alert: %s", exc)


# ── Deficiency alert (no exception on file) ──────────────────────────────────

def queue_deficiency_alert(
    email_queue_table, gc_email: str,
    vendor_name: str, vendor_id: str,
    client_name: str, client_id: str,
    policy_type: str, deficiency_reasons: List[str],
):
    # Disabled — GC views deficiencies in Softr portal, email noise at this stage
    logger.info("Deficiency alert SKIPPED (disabled) for %s re: %s / %s", gc_email, vendor_name, policy_type)
    return


# ── Human override audit trail ───────────────────────────────────────────────

def record_human_override(
    compliance_log_table,
    vendor_id: str,
    vendor_name: str,
    client_id: str,
    client_name: str,
    override_reason: str,
    approved_by: str,
    original_finding: str = "",
):
    """Write a HUMAN_OVERRIDE entry to the Compliance Log.

    Does not modify the original system finding — creates a separate log entry.
    """
    log_entry = f"HUMAN_OVERRIDE — {vendor_name} — {client_name} — {date.today().isoformat()}"

    record = {
        FLD_LOG_ENTRY: log_entry,
        FLD_LOG_TIMESTAMP: _utc_now_iso(),
        FLD_LOG_DECISION: "HUMAN_OVERRIDE",
        FLD_LOG_FAILURE_REASONS: (
            f"Override by: {approved_by}\n"
            f"Reason: {override_reason}\n"
            f"Original finding: {original_finding}"
        ).strip(),
        FLD_LOG_VENDOR_LINK: [vendor_id] if vendor_id else [],
        FLD_LOG_CLIENT_LINK: [client_id] if client_id else [],
    }

    try:
        compliance_log_table.create(record, typecast=True)
        logger.info("Human override logged: %s by %s", log_entry, approved_by)
    except Exception as exc:
        logger.error("Failed to log human override: %s", exc)
