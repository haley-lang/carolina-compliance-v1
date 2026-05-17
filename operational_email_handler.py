"""
Operational Email Handler — Carolina Compliance Solutions

Handles edge cases at the email intake layer after classification:
- Bounce/auto-reply vendor lookup and alerting
- Duplicate submission detection
- Cloud link extraction and alerting
- OCR confidence gating on extraction results
- Large attachment gating
- Unsupported file type handling

Called from email_monitor.py after classification, before extraction.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from legal_disclaimer import EMAIL_DISCLAIMER

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

logger = logging.getLogger(__name__)

# ── Airtable field IDs ───────────────────────────────────────────────────────

# Incoming Documents
FLD_DOC_CLOUD_LINKS      = "fldYIbZz9edaxB2n3"
FLD_DOC_BOUNCE_VENDOR    = "fldyGQoSlNED7wZA6"
FLD_DOC_ATTACHMENT_SIZE   = "fldvZzwYD5HVft88E"
FLD_DOC_SKIP_REASON      = "fldqbm19qK1vdMpX7"
FLD_DOC_STATUS           = "fldIQxRQiAcc6tD6Z"
FLD_DOC_SENDER_EMAIL     = "fld4ycFJuXpRCSGMd"

# Incoming Extractions
FLD_EXT_PROC_STATUS      = "fld4KqSQEX32Zenut"
FLD_EXT_NAMED_INSURED    = "fld4X90MLBQIqTNTn"
FLD_EXT_POLICY_NUMBER    = "flduz1v47MZ90rGy9"

# Vendors
FLD_VEN_EMAIL            = "fldxteHtQ5ITcx6Zw"
FLD_VEN_NAME             = "fldb0BUb3wggDMJMp"

# Email Queue
FLD_EQ_PRIMARY_EMAIL     = "fldWz9aK1If5Z7g3S"
FLD_EQ_SUBJECT           = "fld6v3zOlaNcvcqch"
FLD_EQ_BODY              = "fldvZ7SpNXCoheLpp"
FLD_EQ_EMAIL_TYPE        = "fldtVfbc7XNSVG1pT"
FLD_EQ_REMINDER_STATUS   = "fldzgNFcN8GtcPXt0"
FLD_EQ_SEND_AFTER        = "fldFkI7vgU9PGl92v"
FLD_EQ_CLIENT            = "fldbTJdW79X9G3kal"

# ── Triage / internal review field IDs ────────────────────────────────────────
FLD_DOC_INTERNAL_REVIEW_AT = "fldEgEFr2VuS1ylTh"
FLD_CERT_REVIEW_STATUS     = "fldO3ae2dBvoavMFN"

# Email Queue (for bounce verification)
FLD_EQ_EMAIL_STATUS        = "fldKmLPpaEbYXV8Vw"
FLD_EQ_VENDOR_LINK         = "fldvEsppIZASymJjF"

TRIAGE_ESCALATION_HOURS = 48

# ── Constants ────────────────────────────────────────────────────────────────

MAX_ATTACHMENT_SIZE_MB = 10.0
SUPPORTED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif"}
DUPLICATE_WINDOW_DAYS = 30

import config as _cfg
OWNER_EMAIL = _cfg.OWNER_EMAIL

CLOUD_LINK_PATTERNS = [
    r"(https?://drive\.google\.com/\S+)",
    r"(https?://www\.dropbox\.com/\S+)",
    r"(https?://\S+\.sharepoint\.com/\S+)",
    r"(https?://onedrive\.live\.com/\S+)",
    r"(https?://\S*box\.com/\S+)",
    r"(https?://\S*docusign\.com/\S+)",
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _queue_internal_alert(email_queue_table, subject: str, body: str, email_type: str = "Operational Alert"):
    """Queue an internal alert email to Haley (not the GC)."""
    fields = {
        FLD_EQ_PRIMARY_EMAIL: OWNER_EMAIL,
        FLD_EQ_SUBJECT: subject,
        FLD_EQ_BODY: body,
        FLD_EQ_EMAIL_TYPE: email_type,
        FLD_EQ_REMINDER_STATUS: "Queued",
        FLD_EQ_SEND_AFTER: _utc_now_iso(),
    }
    try:
        email_queue_table.create(fields, typecast=True)
        logger.info("Internal alert queued: %s", subject[:80])
    except Exception as exc:
        logger.error("Failed to queue internal alert: %s", exc)


def set_pending_internal_review(doc_table, doc_record_id: str, reason: str):
    """Route a record to Pending Internal Review instead of alerting the GC.

    Sets Status to Pending Internal Review and records the timestamp.
    Items in this queue go to Haley first. If untouched after 48 hours,
    they auto-escalate to the GC.
    """
    try:
        doc_table.update(doc_record_id, {
            FLD_DOC_STATUS: "Pending Internal Review",
            FLD_DOC_SKIP_REASON: reason,
            FLD_DOC_INTERNAL_REVIEW_AT: _utc_now_iso(),
        }, typecast=True)
        logger.info("Record %s routed to Pending Internal Review: %s", doc_record_id, reason[:80])
    except Exception as exc:
        logger.error("Failed to set Pending Internal Review on %s: %s", doc_record_id, exc)


def check_auto_escalation(doc_table, email_queue_table):
    """Daily cron: escalate Pending Internal Review records older than 48 hours.

    If untouched, set Status to Escalated and queue the GC alert.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=TRIAGE_ESCALATION_HOURS)).isoformat()

    try:
        formula = (
            f"AND("
            f"{{{FLD_DOC_STATUS}}} = 'Pending Internal Review', "
            f"IS_BEFORE({{{FLD_DOC_INTERNAL_REVIEW_AT}}}, '{cutoff}')"
            f")"
        )
        records = doc_table.all(formula=formula)
    except Exception as exc:
        logger.error("Failed to fetch records for auto-escalation: %s", exc)
        return

    escalated = 0
    for record in records:
        record_id = record["id"]
        fields = record.get("fields", {})
        skip_reason = fields.get("Skip Reason") or fields.get(FLD_DOC_SKIP_REASON) or ""
        sender = fields.get("Sender Email") or fields.get(FLD_DOC_SENDER_EMAIL) or ""

        try:
            doc_table.update(record_id, {
                FLD_DOC_STATUS: "Escalated",
            }, typecast=True)
        except Exception as exc:
            logger.error("Failed to escalate record %s: %s", record_id, exc)
            continue

        _queue_internal_alert(
            email_queue_table,
            subject=f"Auto-Escalated: Unreviewed item from {sender}",
            body=(
                f"An item has been in Pending Internal Review for over {TRIAGE_ESCALATION_HOURS} hours "
                f"without action.\n\n"
                f"From: {sender}\n"
                f"Reason: {skip_reason}\n\n"
                f"Please review in Airtable Incoming Documents and take action.\n\n"
                f"Carolina Compliance Solutions"
                f"{EMAIL_DISCLAIMER}"
            ),
            email_type="Escalation Alert",
        )
        escalated += 1

    if escalated:
        logger.info("Auto-escalation: %d records escalated", escalated)


# ── 1. Bounce vendor lookup ──────────────────────────────────────────────────

def handle_bounce(
    doc_table, doc_record_id: str,
    sender_email: str,
    vendors_table,
    email_queue_table,
):
    """When a bounce is detected, check if it's for a known vendor with prior sends.

    Only flag as a problem if:
    1. The bounced address matches a vendor email, AND
    2. There is a prior successful send to that address in the Email Queue

    If no prior send exists, this is likely a generic auto-reply — skip silently.
    """
    bounced_address = sender_email.strip().lower()

    # Find matching vendor
    matched_vendor = None
    try:
        all_vendors = vendors_table.all()
        for v in all_vendors:
            vendor_email = (v["fields"].get("Vendor Email") or v["fields"].get(FLD_VEN_EMAIL) or "").strip().lower()
            if vendor_email and vendor_email in bounced_address:
                matched_vendor = v
                break
    except Exception as exc:
        logger.warning("Vendor lookup for bounce failed: %s", exc)

    if not matched_vendor:
        try:
            doc_table.update(doc_record_id, {
                FLD_DOC_SKIP_REASON: "Auto-reply from non-vendor address",
            })
        except Exception:
            pass
        logger.info("Bounce from non-vendor address %s — skipped", bounced_address[:50])
        return

    vendor_name = matched_vendor["fields"].get("Vendor Name") or matched_vendor["fields"].get(FLD_VEN_NAME) or ""
    vendor_email = matched_vendor["fields"].get("Vendor Email") or matched_vendor["fields"].get(FLD_VEN_EMAIL) or ""

    # Check for prior successful send to this vendor
    has_prior_send = False
    try:
        safe_email = vendor_email.replace("'", "\\'")
        formula = f"AND({{Primary Email}} = '{safe_email}', {{Email Status}} = 'Sent')"
        prior_sends = email_queue_table.all(formula=formula, max_records=1)
        has_prior_send = len(prior_sends) > 0
    except Exception as exc:
        logger.warning("Prior send check failed for %s: %s", vendor_email, exc)

    if not has_prior_send:
        try:
            doc_table.update(doc_record_id, {
                FLD_DOC_SKIP_REASON: f"Auto-reply from {vendor_name} — no prior send on record",
                FLD_DOC_BOUNCE_VENDOR: vendor_name,
            })
        except Exception:
            pass
        logger.info("Bounce for %s but no prior send — skipped", vendor_name)
        return

    # Known vendor with prior send — real bounce, alert internally
    try:
        doc_table.update(doc_record_id, {FLD_DOC_BOUNCE_VENDOR: vendor_name})
    except Exception:
        pass

    _queue_internal_alert(
        email_queue_table,
        subject=f"Delivery Failure: {vendor_name} — email may be invalid",
        body=(
            f"A delivery failure (bounce) was received for an email address associated "
            f"with one of your subcontractors.\n\n"
            f"Vendor: {vendor_name}\n"
            f"Email address: {vendor_email}\n\n"
            f"This email address may be invalid or the mailbox may be full. "
            f"Please verify the vendor's contact information.\n\n"
            f"No emails will be automatically retried to this address.\n\n"
            f"Carolina Compliance Solutions"
            f"{EMAIL_DISCLAIMER}"
        ),
        email_type="Bounce Alert",
    )


# ── 1b. No attachment handling ────────────────────────────────────────────────

def handle_no_attachment(
    doc_table, doc_record_id: str,
    sender_email: str,
    email_queue_table,
):
    """When an email arrives with no attachment, set status and alert the GC owner."""
    try:
        doc_table.update(doc_record_id, {
            FLD_DOC_STATUS: "No Attachment",
            FLD_DOC_SKIP_REASON: "Email received with no qualifying attachment.",
        }, typecast=True)
    except Exception as exc:
        logger.error("Failed to set No Attachment status on %s: %s", doc_record_id, exc)

    _queue_internal_alert(
        email_queue_table,
        subject=f"Email Received With No Attachment — {sender_email}",
        body=(
            f"An email was received with no certificate attachment.\n\n"
            f"From: {sender_email}\n\n"
            f"If this was a COI submission, please ask the vendor to resend "
            f"with the certificate attached as a PDF.\n\n"
            f"Carolina Compliance Solutions"
            f"{EMAIL_DISCLAIMER}"
        ),
        email_type="No Attachment Alert",
    )
    logger.info("No attachment handler: record %s from %s", doc_record_id, sender_email[:50])


# ── 2. Duplicate submission detection ────────────────────────────────────────

def check_duplicate_extraction(
    extractions_table,
    named_insured: str,
    policy_numbers: List[str],
    expiration_dates: List[str],
) -> Optional[Dict[str, Any]]:
    """Check if an identical extraction was processed within the last 30 days.

    Identical = same named insured + same policy numbers + same expiration dates.
    Returns the prior record if duplicate found, None otherwise.
    """
    if not named_insured:
        return None

    cutoff = (datetime.now(timezone.utc) - timedelta(days=DUPLICATE_WINDOW_DAYS)).isoformat()

    try:
        safe_insured = named_insured.replace("'", "\\'")
        formula = (
            f"AND("
            f"{{{FLD_EXT_NAMED_INSURED}}} = '{safe_insured}', "
            f"IS_AFTER({{Extraction Processed At}}, '{cutoff}')"
            f")"
        )
        records = extractions_table.all(formula=formula)

        for record in records:
            fields = record.get("fields", {})
            # Check policy number match
            record_raw = fields.get("Raw JSON", "")
            if not record_raw:
                continue

            try:
                record_data = json.loads(record_raw)
            except (json.JSONDecodeError, TypeError):
                continue

            record_policies = record_data.get("policies", [])
            record_numbers = sorted([
                (p.get("policy_number") or "").strip().lower()
                for p in record_policies if p.get("policy_number")
            ])
            record_expirations = sorted([
                (p.get("expiration_date") or "").strip()
                for p in record_policies if p.get("expiration_date")
            ])

            new_numbers = sorted([n.strip().lower() for n in policy_numbers if n.strip()])
            new_expirations = sorted([d.strip() for d in expiration_dates if d.strip()])

            if record_numbers == new_numbers and record_expirations == new_expirations:
                return record

    except Exception as exc:
        logger.warning("Duplicate check failed: %s", exc)

    return None


# ── 3. Cloud link extraction ─────────────────────────────────────────────────

def extract_cloud_links(body_text: str) -> List[str]:
    """Extract cloud storage URLs from email body text."""
    links = []
    for pattern in CLOUD_LINK_PATTERNS:
        matches = re.findall(pattern, body_text, re.IGNORECASE)
        links.extend(matches)
    return list(dict.fromkeys(links))  # dedupe preserving order


def handle_cloud_link(
    doc_table, doc_record_id: str,
    body_text: str,
    sender_email: str,
    email_queue_table,
    vendor_name: str = "",
):
    """Extract cloud links, write to record, and alert the GC."""
    links = extract_cloud_links(body_text)
    if not links:
        return

    links_text = "\n".join(links)
    sender_display = vendor_name or sender_email

    try:
        doc_table.update(doc_record_id, {
            FLD_DOC_CLOUD_LINKS: links_text,
        })
    except Exception as exc:
        logger.warning("Failed to write cloud links: %s", exc)

    set_pending_internal_review(
        doc_table, doc_record_id,
        f"Cloud link submission from {sender_display}. Links: {links_text[:200]}"
    )


# ── 5. Large attachment gating ───────────────────────────────────────────────

def check_attachment_size(file_path: str) -> float:
    """Return file size in MB."""
    try:
        return Path(file_path).stat().st_size / (1024 * 1024)
    except (OSError, FileNotFoundError):
        return 0.0


def gate_oversize_attachment(
    doc_table, doc_record_id: str,
    file_path: str, size_mb: float,
    sender_email: str,
    email_queue_table,
) -> bool:
    """If attachment over limit, mark as Oversize and alert. Returns True if gated."""
    if size_mb <= MAX_ATTACHMENT_SIZE_MB:
        return False

    try:
        doc_table.update(doc_record_id, {
            FLD_DOC_STATUS: "Oversize",
            FLD_DOC_ATTACHMENT_SIZE: round(size_mb, 2),
            FLD_DOC_SKIP_REASON: (
                f"Attachment {Path(file_path).name} is {size_mb:.1f}MB, "
                f"exceeding the {MAX_ATTACHMENT_SIZE_MB}MB limit."
            ),
        }, typecast=True)
    except Exception as exc:
        logger.error("Failed to write oversize status: %s", exc)

    set_pending_internal_review(
        doc_table, doc_record_id,
        f"Oversized attachment: {Path(file_path).name} is {size_mb:.1f}MB (limit {MAX_ATTACHMENT_SIZE_MB}MB)"
    )

    logger.info("Attachment gated: %s is %.1fMB (limit %.1fMB)",
                file_path, size_mb, MAX_ATTACHMENT_SIZE_MB)
    return True


# ── 6. Unsupported file type handling ────────────────────────────────────────

def is_supported_file_type(filename: str) -> bool:
    """Check if the file extension is supported for extraction."""
    ext = Path(filename).suffix.lower()
    return ext in SUPPORTED_EXTENSIONS


def gate_unsupported_type(
    doc_table, doc_record_id: str,
    filename: str, sender_email: str,
    email_queue_table,
) -> bool:
    """If file type unsupported, mark and alert. Returns True if gated."""
    if is_supported_file_type(filename):
        return False

    ext = Path(filename).suffix.lower() or "(no extension)"
    supported_list = ", ".join(sorted(SUPPORTED_EXTENSIONS))

    try:
        doc_table.update(doc_record_id, {
            FLD_DOC_STATUS: "Unsupported Format",
            FLD_DOC_SKIP_REASON: (
                f"File '{filename}' has unsupported format '{ext}'. "
                f"Supported: {supported_list}."
            ),
        }, typecast=True)
    except Exception as exc:
        logger.error("Failed to write unsupported format status: %s", exc)

    set_pending_internal_review(
        doc_table, doc_record_id,
        f"Unsupported file format: {filename} ({ext}). Supported: {supported_list}"
    )

    logger.info("File gated: %s has unsupported type %s", filename, ext)
    return True
