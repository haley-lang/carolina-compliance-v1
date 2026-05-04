"""
Module 8B: Cancellation / Endorsement / Reinstatement Action Handler

Runs after the main processor. Scans Incoming Extractions for any unprocessed
records where Document Type is cancellation_notice, endorsement, or reinstatement,
then takes action:

  cancellation_notice → set vendor Non-Compliant, queue 3 emails:
      1. Haley internal alert
      2. GC client alert
      3. Sub + agency reinstatement request

  endorsement → re-trigger module_7b compliance check for the matched vendor/client,
      queue internal alert for Haley to review

  reinstatement → re-trigger module_7b compliance check, potentially restore Compliant
      status, queue internal alert
"""

import os
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from pyairtable import Api
from dotenv import load_dotenv
from legal_disclaimer import EMAIL_DISCLAIMER
from email_template import build_email_html

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Airtable config ──────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

import config as _cfg
HALEY_EMAIL = _cfg.OWNER_EMAIL


def _plain_to_html(text: str) -> str:
    """Convert a plain-text email body to simple HTML paragraphs."""
    return "<p>" + text.replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>"

# ── Table IDs ────────────────────────────────────────────────────────────────
TABLE_EXTRACTIONS     = "tblT88Ty6d6M766oY"   # Incoming Extractions
TABLE_VENDORS         = "tblsOphSd5DKSZEro"   # Vendors
TABLE_CLIENTS         = "tbltnBIWke20IEI3K"   # Clients
TABLE_ASSIGNMENTS     = "tblpYKywfs0YHiQ98"   # Vendor Client Assignments
TABLE_CLIENT_REQS     = "tblFGQ6XgOIHSWtQN"   # Client Requirements
TABLE_POLICIES        = "tblpPcmm5ANE0bMNB"   # Insurance Policies
TABLE_EMAIL_QUEUE     = "tblCeRCf6RToTFkbL"   # Email Queue

# ── Field IDs: Incoming Extractions ──────────────────────────────────────────
FLD_EX_DOC_TYPE       = "fldovqDwNx7SnTkcm"   # Document Type (singleSelect)
FLD_EX_ACTION_TAKEN   = "fldlvQXsy7c12E9ak"   # Cancellation Action Taken (checkbox)
FLD_EX_MATCHED_VENDOR = "fldOku9CPphnETmTA"   # Matched Vendor (link)
FLD_EX_MATCHED_CLIENT = "fld226LVbYVQnaM2i"   # Matched Client (link)
FLD_EX_NAMED_INSURED  = "fld4X90MLBQIqTNTn"   # Named Insured
FLD_EX_CONTACT_EMAILS = "fldzuzzd0hlvuVRN6"   # Contact Emails
FLD_EX_PROC_STATUS    = "fld4KqSQEX32Zenut"   # Processing Status
FLD_EX_SOURCE_FILE    = "fldHAwdxnX3yM0s3o"   # Source Filename

# ── Field IDs: Vendors ────────────────────────────────────────────────────────
FLD_VEN_NAME          = "fldb0BUb3wggDMJMp"
FLD_VEN_EMAIL         = "fldxteHtQ5ITcx6Zw"
FLD_VEN_AGENCY_EMAIL  = "fldWfYjoWXxksW5iG"   # Agency Email (new field)
FLD_VEN_COMP_STATUS   = "fldUSiQOqZocnT4zX"   # Compliance Status

# ── Field IDs: Clients ────────────────────────────────────────────────────────
FLD_CLI_NAME          = "fldEZdqmIeahXDZHL"
FLD_CLI_CONTACT_EMAIL = "fldmh1sYahgN5x6KQ"
FLD_CLI_CONTACT_NAME  = "fldIWXSLRJYAVRs3P"

# ── Field IDs: Vendor Client Assignments ─────────────────────────────────────
FLD_ASSIGN_VENDOR     = "fld7ZjWTE652bGJve"
FLD_ASSIGN_CLIENT     = "fldT229klsR59yvqz"
FLD_ASSIGN_ACTIVE     = "fld7aAB0tYKJLzrEw"
FLD_ASSIGN_COMP       = "fldC8Iqd488Q9Y6nl"
FLD_ASSIGN_EVALUATED  = "fldueiS5Hq8s5MYLW"

# ── Field IDs: Email Queue ────────────────────────────────────────────────────
FLD_EQ_PRIMARY_EMAIL  = "fldWz9aK1If5Z7g3S"
FLD_EQ_CC_EMAILS      = "fldnF0XlINSorsQFG"
FLD_EQ_SUBJECT        = "fld6v3zOlaNcvcqch"
FLD_EQ_BODY           = "fldvZ7SpNXCoheLpp"
FLD_EQ_EMAIL_STATUS   = "fldKmLPpaEbYXV8Vw"
FLD_EQ_EMAIL_TYPE     = "fldtVfbc7XNSVG1pT"
FLD_EQ_VENDOR_LINK    = "fldvEsppIZASymJjF"
FLD_EQ_CLIENT_LINK    = "fldbTJdW79X9G3kal"
FLD_EQ_CREATED_AT     = "flduah8p5oIxULYHN"
FLD_EQ_RECORD_STATUS  = "fld98bdjeVxJPyBDj"
FLD_EQ_SEND_AFTER     = "fldFkI7vgU9PGl92v"

# ── Cancellation subtype field IDs (Incoming Extractions) ────────────────────
FLD_EX_CANCEL_SUBTYPE   = "fldYEvqxi5ueyBCon"
FLD_EX_AFFECTED_LINES   = "fldXrm2oMs6ZJQk2P"
FLD_EX_CANCEL_EFF_DATE  = "fldUjNYHxtaVhhYDY"
FLD_EX_CANCEL_CASE      = "fldcvp5ljbSskvrdS"
FLD_EX_CANCEL_RESOLVED  = "fldKub7zPYAmttGqT"
FLD_EX_POLICY_NUMBER    = "flduz1v47MZ90rGy9"

FLD_EQ_CC_EMAILS        = "fldnF0XlINSorsQFG"

FLD_VEN_AGENCY_EMAIL_ID = "fldWfYjoWXxksW5iG"

# ── Cancellation subtype constants ───────────────────────────────────────────
CANCEL_FULL              = "full_cancellation"
CANCEL_INTENT            = "cancellation_intent"
CANCEL_PREMIUM_FINANCE   = "premium_finance_cancellation"
CANCEL_PARTIAL           = "partial_cancellation"

import re as _re

# Patterns for cancellation subtype detection
_PREMIUM_FINANCE_PATTERNS = [
    r"premium\s+financ",
    r"premium\s+funding",
    r"financed\s+premium",
]

_INTENT_PATTERNS = [
    r"notice\s+of\s+intent\s+to\s+cancel",
    r"intent\s+to\s+cancel",
    r"proposed\s+cancellation",
    r"pending\s+cancellation",
]

_FINAL_CANCEL_PATTERNS = [
    r"cancellation\s+effective",
    r"cancelled\s+effective",
    r"canceled\s+effective",
    r"has\s+been\s+cancelled",
    r"has\s+been\s+canceled",
    r"policy\s+cancelled",
    r"policy\s+canceled",
]

_POLICY_LINE_PATTERNS = {
    "General Liability": [r"general\s+liab", r"\bCGL\b", r"commercial\s+general"],
    "Auto Liability":    [r"auto\s+liab", r"automobile\s+liab", r"commercial\s+auto"],
    "Workers Comp":      [r"workers?\s*comp", r"\bWC\b", r"employers?\s+liab"],
    "Umbrella":          [r"umbrella", r"excess\s+liab"],
}


_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y")

def _parse_date_safe(raw) -> date:
    """Parse a date string, return None on failure."""
    cleaned = (str(raw) if raw else "").strip()
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _extract_cancellation_effective_date(extraction_fields: dict) -> str:
    """Try to extract a cancellation effective date from extraction data.

    Looks in Raw JSON for expiration_date fields on the cancelled policy,
    and also searches for date patterns near cancellation keywords.
    """
    import json as _json

    raw_json_str = extraction_fields.get("Raw JSON") or ""
    try:
        raw_data = _json.loads(raw_json_str) if raw_json_str else {}
    except (_json.JSONDecodeError, TypeError):
        raw_data = {}

    # Check policies for expiration dates — the cancellation effective date
    # is often the expiration date on the cancelled policy
    policies = raw_data.get("policies") or []
    for p in policies:
        exp = (p.get("expiration_date") or "").strip()
        if exp and _parse_date_safe(exp):
            return exp

    # Search raw text for date patterns near "cancel" or "effective"
    combined = raw_json_str.lower()
    date_patterns = _re.findall(r"(\d{1,2}/\d{1,2}/\d{2,4})", combined)
    if date_patterns:
        for dp in date_patterns:
            parsed = _parse_date_safe(dp)
            if parsed:
                return dp

    return ""


def _matches_any(text: str, patterns: list) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(_re.search(p, text_lower) for p in patterns)


def _detect_affected_lines(text: str) -> list:
    """Detect which specific policy lines are mentioned in a cancellation notice."""
    if not text:
        return []
    text_lower = text.lower()
    affected = []
    for line_name, patterns in _POLICY_LINE_PATTERNS.items():
        if any(_re.search(p, text_lower) for p in patterns):
            affected.append(line_name)
    return affected


def classify_cancellation_subtype(extraction_fields: dict) -> tuple:
    """Classify a cancellation notice into a subtype.

    Returns (subtype, affected_lines) where affected_lines is a list of
    policy line names for partial cancellations, or empty for full.
    """
    named_insured = extraction_fields.get(FLD_EX_NAMED_INSURED) or ""
    raw_json = extraction_fields.get("Raw JSON") or ""
    source_file = extraction_fields.get(FLD_EX_SOURCE_FILE) or ""

    # Combine all available text for pattern matching
    combined = f"{named_insured} {raw_json} {source_file}"

    # Priority 1: Premium finance cancellation
    if _matches_any(combined, _PREMIUM_FINANCE_PATTERNS):
        return CANCEL_PREMIUM_FINANCE, []

    # Priority 2: Intent vs final
    is_intent = _matches_any(combined, _INTENT_PATTERNS)
    is_final = _matches_any(combined, _FINAL_CANCEL_PATTERNS)

    if is_intent and not is_final:
        return CANCEL_INTENT, []

    # Priority 3: Partial line cancellation
    affected = _detect_affected_lines(combined)
    if affected and len(affected) < 3:
        return CANCEL_PARTIAL, affected

    # Default: full cancellation
    return CANCEL_FULL, []


def fetch_unprocessed_events(extractions_table):
    """Return all Incoming Extraction records where Document Type is
    cancellation_notice, endorsement, or reinstatement AND
    Cancellation Action Taken is False/unchecked."""
    records = extractions_table.all()
    actionable_types = {"cancellation_notice", "endorsement", "reinstatement"}
    result = []
    for r in records:
        doc_type = (r["fields"].get(FLD_EX_DOC_TYPE) or {})
        if isinstance(doc_type, dict):
            doc_type = doc_type.get("name", "")
        doc_type = doc_type.strip().lower()
        action_taken = r["fields"].get(FLD_EX_ACTION_TAKEN, False)
        if doc_type in actionable_types and not action_taken:
            result.append(r)
    logger.info("Found %d unprocessed cancellation/endorsement/reinstatement records", len(result))
    return result


def get_record(table, record_id):
    try:
        return table.get(record_id)
    except Exception as e:
        logger.error("Failed to fetch record %s: %s", record_id, e)
        return None


def queue_email(email_queue_table, primary_email, subject, body, email_type,
                cc_emails=None, vendor_id=None, client_id=None):
    """Insert a record into the Email Queue table. Appends legal disclaimer."""
    body_with_disclaimer = body + EMAIL_DISCLAIMER
    fields = {
        FLD_EQ_PRIMARY_EMAIL: primary_email,
        FLD_EQ_SUBJECT:       subject,
        FLD_EQ_BODY:          body_with_disclaimer,
        FLD_EQ_EMAIL_TYPE:    email_type,
        FLD_EQ_EMAIL_STATUS:  "Pending",
        FLD_EQ_RECORD_STATUS: "Active",
        FLD_EQ_CREATED_AT:    datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        FLD_EQ_SEND_AFTER:    datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    if cc_emails:
        fields[FLD_EQ_CC_EMAILS] = cc_emails
    if vendor_id:
        fields[FLD_EQ_VENDOR_LINK] = [vendor_id]
    if client_id:
        fields[FLD_EQ_CLIENT_LINK] = [client_id]
    try:
        email_queue_table.create(fields)
        logger.info("Queued %s email to %s", email_type, primary_email)
    except Exception as e:
        logger.error("Failed to queue email to %s: %s", primary_email, e)


def handle_cancellation(extraction, vendor, client, email_queue_table,
                        vendors_table, assignments_table, extractions_table=None):
    """Cancellation workflow driven by effective date:
    1. Extract cancellation effective date
    2. Classify subtype (full, intent, premium finance, partial)
    3. Email agency immediately (CC sub)
    4. Email GC with timing-appropriate language
    5. Set case status to Monitoring if future-dated, Open if past/today
    """
    vendor_id    = vendor["id"]
    vendor_name  = vendor["fields"].get(FLD_VEN_NAME, "Unknown Vendor")
    vendor_email = vendor["fields"].get(FLD_VEN_EMAIL, "")
    agency_email = (
        vendor["fields"].get(FLD_VEN_AGENCY_EMAIL)
        or vendor["fields"].get(FLD_VEN_AGENCY_EMAIL_ID)
        or ""
    )

    client_id      = client["id"]
    client_name    = client["fields"].get(FLD_CLI_NAME, "Unknown Client")
    client_email   = client["fields"].get(FLD_CLI_CONTACT_EMAIL, "")
    client_contact = client["fields"].get(FLD_CLI_CONTACT_NAME, "")

    source_file = extraction["fields"].get(FLD_EX_SOURCE_FILE, "unknown document")
    today = date.today()
    today_str = today.strftime("%m/%d/%Y")

    # ── Step 1: Extract cancellation effective date ──────────────────────
    eff_date_raw = _extract_cancellation_effective_date(extraction["fields"])
    eff_date = _parse_date_safe(eff_date_raw)
    eff_date_display = eff_date_raw or "(not extracted — requires internal review)"

    # ── Step 2: Classify subtype ─────────────────────────────────────────
    subtype, affected_lines = classify_cancellation_subtype(extraction["fields"])
    logger.info("Cancellation: vendor=%s client=%s subtype=%s eff_date=%s",
                vendor_name, client_name, subtype, eff_date_display)

    # Determine timing
    is_future = eff_date and eff_date > today
    is_past_or_today = eff_date and eff_date <= today
    case_status = "Monitoring" if is_future else "Open"

    # ── Write to extraction record ───────────────────────────────────────
    if extractions_table:
        try:
            update = {
                FLD_EX_CANCEL_SUBTYPE: subtype,
                FLD_EX_CANCEL_CASE: case_status,
            }
            if eff_date:
                update[FLD_EX_CANCEL_EFF_DATE] = eff_date.isoformat()
            if affected_lines:
                update[FLD_EX_AFFECTED_LINES] = "\n".join(affected_lines)
            extractions_table.update(extraction["id"], update, typecast=True)
        except Exception as e:
            logger.error("Failed to write cancellation fields: %s", e)

    # ── Intent only — softer handling, no status change ──────────────────
    if subtype == CANCEL_INTENT:
        try:
            vendors_table.update(vendor_id, {FLD_VEN_COMP_STATUS: "Needs Review"}, typecast=True)
        except Exception:
            pass

        queue_email(
            email_queue_table,
            primary_email=HALEY_EMAIL,
            subject=f"⚠️ Notice of Intent to Cancel: {vendor_name} ({client_name})",
            body=(
                f"A notice of intent to cancel has been received.\n\n"
                f"Subcontractor: {vendor_name}\n"
                f"Client: {client_name}\n"
                f"Detected: {today_str}\n"
                f"Source: {source_file}\n\n"
                f"This is an intent notice, not a confirmed cancellation. "
                f"Please follow up with the vendor or their broker to confirm "
                f"whether cancellation will proceed.\n\n"
                f"Carolina Compliance Solutions"
            ),
            email_type="Cancellation Alert",
            vendor_id=vendor_id,
            client_id=client_id,
        )
        return

    # ── Partial cancellation — flag specific lines, Needs Review ─────────
    if subtype == CANCEL_PARTIAL and affected_lines:
        try:
            vendors_table.update(vendor_id, {FLD_VEN_COMP_STATUS: "Needs Review"}, typecast=True)
        except Exception:
            pass

        lines_text = ", ".join(affected_lines)
        queue_email(
            email_queue_table,
            primary_email=HALEY_EMAIL,
            subject=f"⚠️ Partial Cancellation: {vendor_name} — {lines_text}",
            body=(
                f"A cancellation notice affecting specific policy lines has been received.\n\n"
                f"Subcontractor: {vendor_name}\n"
                f"Client: {client_name}\n"
                f"Affected lines: {lines_text}\n"
                f"Effective date: {eff_date_display}\n"
                f"Source: {source_file}\n\n"
                f"Other policy lines appear unaffected based on submitted documentation.\n\n"
                f"Carolina Compliance Solutions"
            ),
            email_type="Cancellation Alert",
            vendor_id=vendor_id,
            client_id=client_id,
        )
        # Still send agency/sub notification for partial
        # (falls through to agency email below)

    # ── Full / premium finance: update vendor + assignment status ─────────
    if subtype in (CANCEL_FULL, CANCEL_PREMIUM_FINANCE):
        urgency = ""
        if subtype == CANCEL_PREMIUM_FINANCE:
            urgency = "\n⚡ URGENT: Premium finance cancellation — shorter notice period.\n"

        try:
            vendors_table.update(vendor_id, {FLD_VEN_COMP_STATUS: "Has Open Items"}, typecast=True)
        except Exception as e:
            logger.error("Failed to update vendor status: %s", e)

        try:
            assignments = assignments_table.all()
            for a in assignments:
                a_vendor = (a["fields"].get(FLD_ASSIGN_VENDOR) or [None])[0]
                a_client = (a["fields"].get(FLD_ASSIGN_CLIENT) or [None])[0]
                if a_vendor == vendor_id and a_client == client_id and a["fields"].get(FLD_ASSIGN_ACTIVE):
                    assignments_table.update(a["id"], {
                        FLD_ASSIGN_COMP: "Has Open Items",
                        FLD_ASSIGN_EVALUATED: today.isoformat(),
                    }, typecast=True)
        except Exception as e:
            logger.error("Failed to update assignments: %s", e)

        # Internal alert to Haley
        queue_email(
            email_queue_table,
            primary_email=HALEY_EMAIL,
            subject=f"⚠️ Cancellation: {vendor_name} ({client_name})",
            body=(
                f"Cancellation notice processed.\n\n"
                f"Subcontractor: {vendor_name}\n"
                f"Client: {client_name}\n"
                f"Type: {subtype.replace('_', ' ').title()}\n"
                f"Effective date: {eff_date_display}\n"
                f"Case status: {case_status}\n"
                f"Source: {source_file}\n"
                f"{urgency}\n"
                f"Agency and vendor have been notified. GC notification sent.\n\n"
                f"Carolina Compliance Solutions"
            ),
            email_type="Cancellation Alert",
            vendor_id=vendor_id,
            client_id=client_id,
        )

    # ── Step 3: Immediate agency + sub notification (all non-intent) ─────
    primary_notify = agency_email or vendor_email
    cc_notify = vendor_email if agency_email and vendor_email != agency_email else None
    policy_number = extraction["fields"].get(FLD_EX_POLICY_NUMBER) or extraction["fields"].get("Policy Number") or "(not identified)"

    if primary_notify:
        queue_email(
            email_queue_table,
            primary_email=primary_notify,
            subject=f"Cancellation Notice Received — {vendor_name} / {client_name}",
            body=(
                f"We have received a notice of cancellation effective {eff_date_display} "
                f"for policy {policy_number} on behalf of {client_name}.\n\n"
                f"Please advise on the status of this policy at your earliest convenience.\n\n"
                f"If the policy has been reinstated or replaced, please send an updated "
                f"Certificate of Insurance to:\n"
                f"{_cfg.INBOUND_EMAIL}\n\n"
                f"Carolina Compliance Solutions\n"
                f"{_cfg.INBOUND_EMAIL}"
            ),
            email_type="Reinstatement Request",
            cc_emails=cc_notify,
            vendor_id=vendor_id,
            client_id=client_id,
        )

    # ── Step 4: GC notification with timing-appropriate language ──────────
    if client_email and subtype in (CANCEL_FULL, CANCEL_PREMIUM_FINANCE):
        if is_past_or_today:
            gc_body = (
                f"Dear {client_contact or client_name},\n\n"
                f"A cancellation notice has been received for the following subcontractor:\n\n"
                f"Subcontractor: {vendor_name}\n"
                f"Cancellation effective date: {eff_date_display}\n\n"
                f"The cancellation effective date on the submitted notice has passed. "
                f"Submitted documentation indicates this policy is no longer in effect "
                f"as of the effective date. Please consult your insurance advisor for "
                f"coverage determinations.\n\n"
                f"We have contacted the vendor and their agency to request updated "
                f"certificate documentation.\n\n"
                f"Carolina Compliance Solutions\n"
                f"{_cfg.OWNER_EMAIL}"
            )
        else:
            gc_body = (
                f"Dear {client_contact or client_name},\n\n"
                f"We received a cancellation notice for the following subcontractor:\n\n"
                f"Subcontractor: {vendor_name}\n"
                f"Cancellation effective date: {eff_date_display}\n\n"
                f"We have requested updated certificate documentation from the vendor "
                f"and their agency. We will notify you if updated documentation is not "
                f"received before the effective date.\n\n"
                f"Carolina Compliance Solutions\n"
                f"{_cfg.OWNER_EMAIL}"
            )

        queue_email(
            email_queue_table,
            primary_email=client_email,
            subject=f"Certificate Alert: {vendor_name} — Cancellation Notice",
            body=gc_body,
            email_type="Cancellation Alert",
            vendor_id=vendor_id,
            client_id=client_id,
        )


def check_cancellation_monitoring(extractions_table, email_queue_table, clients_table, vendors_table):
    """Daily cron: check Monitoring cancellation cases where effective date has passed.

    If effective date passed with no resolution, escalate and notify GC.
    Called from run_pipeline.py or a cron job.
    """
    today = date.today()
    try:
        records = extractions_table.all()
    except Exception as exc:
        logger.error("Failed to fetch extractions for monitoring check: %s", exc)
        return

    escalated = 0
    for record in records:
        fields = record.get("fields", {})
        case_status = fields.get(FLD_EX_CANCEL_CASE) or fields.get("Cancellation Case Status") or ""
        if isinstance(case_status, dict):
            case_status = case_status.get("name", "")

        if case_status != "Monitoring":
            continue

        eff_raw = fields.get(FLD_EX_CANCEL_EFF_DATE) or fields.get("Cancellation Effective Date") or ""
        eff_date = _parse_date_safe(eff_raw)
        if not eff_date or eff_date > today:
            continue

        # Effective date has passed — escalate
        vendor_name = fields.get(FLD_EX_NAMED_INSURED) or fields.get("Named Insured") or "Unknown"
        logger.info("Cancellation monitoring escalation: %s effective date %s has passed", vendor_name, eff_raw)

        try:
            extractions_table.update(record["id"], {
                FLD_EX_CANCEL_CASE: "Escalated",
            }, typecast=True)
        except Exception as exc:
            logger.error("Failed to escalate cancellation case %s: %s", record["id"], exc)

        # Resolve client email for GC notification
        client_ids = fields.get(FLD_EX_MATCHED_CLIENT) or fields.get("Matched Client") or []
        gc_email = ""
        client_name = ""
        if client_ids and clients_table:
            try:
                client_rec = clients_table.get(client_ids[0])
                gc_email = client_rec.get("fields", {}).get("Portal Email") or client_rec.get("fields", {}).get(FLD_CLI_CONTACT_EMAIL) or ""
                client_name = client_rec.get("fields", {}).get(FLD_CLI_NAME) or ""
            except Exception:
                pass

        if gc_email:
            queue_email(
                email_queue_table,
                primary_email=gc_email,
                subject=f"Follow-Up: {vendor_name} — Cancellation Effective Date Passed",
                body=(
                    f"The cancellation effective date for the following subcontractor "
                    f"has passed with no updated certificate documentation received.\n\n"
                    f"Subcontractor: {vendor_name}\n"
                    f"Cancellation effective date: {eff_raw}\n\n"
                    f"Please review accordingly and consult your insurance advisor.\n\n"
                    f"Carolina Compliance Solutions\n"
                    f"{_cfg.OWNER_EMAIL}"
                ),
                email_type="Cancellation Alert",
            )

        escalated += 1

    if escalated:
        logger.info("Cancellation monitoring: %d cases escalated", escalated)


def handle_endorsement(extraction, vendor, client, email_queue_table):
    """
    Endorsements require manual review — queue an internal alert to Haley.
    Module 7b will re-evaluate compliance on next pipeline run via normal flow.
    """
    vendor_name  = vendor["fields"].get(FLD_VEN_NAME, "Unknown Vendor")
    client_name  = client["fields"].get(FLD_CLI_NAME, "Unknown Client")
    source_file  = extraction["fields"].get(FLD_EX_SOURCE_FILE, "unknown document")
    today_str    = date.today().strftime("%m/%d/%Y")

    logger.info("Handling endorsement: vendor=%s client=%s", vendor_name, client_name)

    _endorse_subject = f"📋 Endorsement Received — Review Required: {vendor_name} ({client_name})"
    _endorse_body = (
        f"An endorsement document has been received and requires manual review.\n\n"
        f"Subcontractor: {vendor_name}\n"
        f"Client: {client_name}\n"
        f"Detected: {today_str}\n"
        f"Source Document: {source_file}\n\n"
        f"Endorsements may change coverage limits, remove additional insured status, "
        f"modify waiver of subrogation, or alter other policy terms.\n\n"
        f"Please review the extracted data in Incoming Extractions to determine whether "
        f"the vendor's submitted documentation reflects {client_name}'s certificate requirements.\n\n"
        f"Carolina Compliance Solutions"
    )
    queue_email(
        email_queue_table,
        primary_email=HALEY_EMAIL,
        subject=_endorse_subject,
        body=build_email_html(_endorse_subject, _plain_to_html(_endorse_body), audience="internal"),
        email_type="Endorsement Alert",
        vendor_id=vendor["id"],
        client_id=client["id"],
    )


def handle_reinstatement(extraction, vendor, client, email_queue_table,
                         vendors_table, assignments_table,
                         client_requirements_table, policies_table):
    """
    Re-run compliance check for this vendor+client pair.
    Import module_7b logic inline to avoid circular imports.
    Queue internal alert with result.
    """
    # Import here to avoid circular dependency if modules share config
    try:
        import module_7b_requirement_validator as module_7b
        vendor_records   = [vendor]
        client_reqs      = module_7b.fetch_requirements_for_client(client["id"], client_requirements_table)
        vendor_policies  = module_7b.fetch_policies_for_vendor(vendor["id"], policies_table)
        new_status       = module_7b.validate_vendor(vendor, client_reqs, vendor_policies)
    except Exception as e:
        logger.error("Failed to run compliance check for reinstatement: %s", e)
        new_status = "Needs Review"

    vendor_name  = vendor["fields"].get(FLD_VEN_NAME, "Unknown Vendor")
    client_name  = client["fields"].get(FLD_CLI_NAME, "Unknown Client")
    source_file  = extraction["fields"].get(FLD_EX_SOURCE_FILE, "unknown document")
    today_str    = date.today().strftime("%m/%d/%Y")

    logger.info("Reinstatement result: vendor=%s → %s", vendor_name, new_status)

    # Update vendor compliance status
    try:
        vendors_table.update(vendor["id"], {FLD_VEN_COMP_STATUS: new_status})
    except Exception as e:
        logger.error("Failed to update vendor status after reinstatement: %s", e)

    # Update active assignments
    try:
        assignments = assignments_table.all()
        for a in assignments:
            a_vendor = (a["fields"].get(FLD_ASSIGN_VENDOR) or [None])[0]
            a_client = (a["fields"].get(FLD_ASSIGN_CLIENT) or [None])[0]
            a_active = a["fields"].get(FLD_ASSIGN_ACTIVE, False)
            if a_vendor == vendor["id"] and a_client == client["id"] and a_active:
                assignments_table.update(a["id"], {
                    FLD_ASSIGN_COMP:      new_status,
                    FLD_ASSIGN_EVALUATED: date.today().isoformat(),
                })
    except Exception as e:
        logger.error("Failed to update assignments after reinstatement: %s", e)

    # Alert Haley
    _reinstate_subject = f"✅ Reinstatement Received: {vendor_name} ({client_name}) — Status: {new_status}"
    _reinstate_body = (
        f"A reinstatement document has been received and processed.\n\n"
        f"Subcontractor: {vendor_name}\n"
        f"Client: {client_name}\n"
        f"Detected: {today_str}\n"
        f"Source Document: {source_file}\n\n"
        f"Compliance check result: {new_status}\n\n"
        f"{'Submitted documentation now meets certificate requirements on file.' if new_status == 'Matches Requirements' else 'Outstanding documentation items remain — please review in Airtable.'}\n\n"
        f"Carolina Compliance Solutions"
    )
    queue_email(
        email_queue_table,
        primary_email=HALEY_EMAIL,
        subject=_reinstate_subject,
        body=build_email_html(_reinstate_subject, _plain_to_html(_reinstate_body), audience="internal"),
        email_type="Reinstatement Alert",
        vendor_id=vendor["id"],
        client_id=client["id"],
    )


def mark_action_taken(extractions_table, extraction_id):
    try:
        extractions_table.update(extraction_id, {FLD_EX_ACTION_TAKEN: True})
        logger.info("Marked extraction %s as action taken", extraction_id)
    except Exception as e:
        logger.error("Failed to mark action taken for %s: %s", extraction_id, e)


def run():
    logger.info("=== Module 8B: Cancellation/Endorsement/Reinstatement Handler starting ===")

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        logger.error("Missing Airtable credentials.")
        return

    api = Api(AIRTABLE_API_KEY)
    extractions_table        = api.table(AIRTABLE_BASE_ID, TABLE_EXTRACTIONS)
    vendors_table            = api.table(AIRTABLE_BASE_ID, TABLE_VENDORS)
    clients_table            = api.table(AIRTABLE_BASE_ID, TABLE_CLIENTS)
    assignments_table        = api.table(AIRTABLE_BASE_ID, TABLE_ASSIGNMENTS)
    client_requirements_table = api.table(AIRTABLE_BASE_ID, TABLE_CLIENT_REQS)
    policies_table           = api.table(AIRTABLE_BASE_ID, TABLE_POLICIES)
    email_queue_table        = api.table(AIRTABLE_BASE_ID, TABLE_EMAIL_QUEUE)

    events = fetch_unprocessed_events(extractions_table)

    for extraction in events:
        extraction_id = extraction["id"]
        fields = extraction["fields"]

        doc_type = (fields.get(FLD_EX_DOC_TYPE) or {})
        if isinstance(doc_type, dict):
            doc_type = doc_type.get("name", "")
        doc_type = doc_type.strip().lower()

        # Resolve matched vendor
        vendor_ids = fields.get(FLD_EX_MATCHED_VENDOR) or []
        vendor = get_record(vendors_table, vendor_ids[0]) if vendor_ids else None
        if not vendor:
            logger.warning("Extraction %s has no matched vendor — skipping", extraction_id)
            mark_action_taken(extractions_table, extraction_id)
            continue

        # Resolve matched client
        client_ids = fields.get(FLD_EX_MATCHED_CLIENT) or []
        client = get_record(clients_table, client_ids[0]) if client_ids else None
        if not client:
            logger.warning("Extraction %s has no matched client — skipping", extraction_id)
            mark_action_taken(extractions_table, extraction_id)
            continue

        if doc_type == "cancellation_notice":
            handle_cancellation(extraction, vendor, client, email_queue_table,
                                vendors_table, assignments_table,
                                extractions_table=extractions_table)

        elif doc_type == "endorsement":
            handle_endorsement(extraction, vendor, client, email_queue_table)

        elif doc_type == "reinstatement":
            handle_reinstatement(extraction, vendor, client, email_queue_table,
                                 vendors_table, assignments_table,
                                 client_requirements_table, policies_table)

        mark_action_taken(extractions_table, extraction_id)

    logger.info("=== Module 8B complete. Processed %d events ===", len(events))


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.error("Module 8B failed: %s", exc)
        raise SystemExit(1) from exc
