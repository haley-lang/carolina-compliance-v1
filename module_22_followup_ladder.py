"""Module 22 — Outbound Follow-Up Ladder state machine.

Two ladder types, both run the same Day 0 → Day 2 → Day 4 → Day 6 stage
progression:
  - Short-Dated Renewal (initiated by module_20 when a cert has policies
    expiring within 30 days).
  - Noncompliance Return (initiated by module_7b when an assignment is
    evaluated as Non-Compliant, Needs Review, or Missing Coverage).

Passive response detection halts the ladder when an inbound email from a
recipient references the ladder's named insured. A new compliant cert for
the vendor halts all active ladders for that vendor. Bounces retry to the
PDF contact, then escalate on second failure.

Email Queue ↔ ladder wiring:
  - Each ladder send writes an Email Queue row with:
      Email Type       = "Ladder Day N" (N ∈ {0, 2, 4, 0 Retry, 6 Approval})
      Reminder Reason  = "LADDER:<ladder_record_id>"
  - module_10 reads those two fields on successful send and stamps the
    corresponding "Day N Sent At" on the ladder record.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pyairtable import Api

import config
from module_21_business_hours import (
    ET,
    send_now_or_defer,
    add_business_days,
    is_business_hours,
)


logger = logging.getLogger(__name__)


# ── Airtable IDs ─────────────────────────────────────────────────────────────

# Outbound Follow-Ups (tbl3T8AjTrzrg2PY4)
LADDER_TABLE_ID = "tbl3T8AjTrzrg2PY4"
FLD_LADDER_ID             = "fldUR983KbFAPZ1HW"
FLD_LADDER_TYPE           = "fldSZCykoBzmwzwEy"
FLD_STAGE                 = "fldf7Swb9UKlM9fL2"
FLD_STATUS                = "fldLVASkr3WuKq9wU"
FLD_NAMED_INSURED         = "fld3g2wimvCZfAs8U"
FLD_ORIGINAL_SENDER       = "fldFsbAceX5fUFYtg"
FLD_BODY_REPLY_TO         = "fldb5NiqKO41L2zyP"
FLD_PDF_CONTACT_EMAIL     = "fldFW6FJCMjv6CMCo"
FLD_DAY_0_SENT_AT         = "fldnFGpaFMupfNoKy"
FLD_DAY_2_SEND_AT         = "fldISWg7T7NKRbFTS"
FLD_DAY_2_SENT_AT         = "fld2RtXvbGGRLUo9o"
FLD_DAY_4_SEND_AT         = "fldK8S6GEgCQxeZ3X"
FLD_DAY_4_SENT_AT         = "fldgnwrlvFSvx9oB9"
FLD_DAY_6_ESCALATE_AT     = "fldfR3XEgABgKmnVz"
FLD_DASHBOARD_ESCALATED   = "fldiFZBspEtUnkgfE"
FLD_DASHBOARD_APPROVAL    = "fldQzAfqIEETg6xJC"
FLD_EMAIL_SUBJECT         = "fld5p8mBsp6rj8MV2"
FLD_EMAIL_BODY            = "fldJCEzm4b7YX7h1O"
FLD_BOUNCE_COUNT          = "fldF41Wdtw9T39THi"
FLD_TERMINATION_REASON    = "fld7igKXCFXpNNgul"
FLD_CREATED_AT            = "fldcKijhFqwOX4cri"
FLD_VENDOR_LINK           = "fldlvfmmGr7QydBJR"
FLD_CLIENT_LINK           = "fldZsd5qiLQtbqe77"
FLD_CERTIFICATE_LINK      = "fldbc1Tj9baakjUpj"
FLD_LINKED_EQ_RECORDS     = "fldeQlEqq3MNjE9DJ"

# Clients (tbltnBIWke20IEI3K)
CLIENTS_TABLE_ID = "tbltnBIWke20IEI3K"
FLD_CLIENT_NAME                 = "fldEZdqmIeahXDZHL"
FLD_CLIENT_PRIMARY_CONTACT_EMAIL = "fldmh1sYahgN5x6KQ"
FLD_CLIENT_PRIMARY_CONTACT_NAME  = "fldIWXSLRJYAVRs3P"
FLD_CLIENT_COI_CONTACT_NAME      = "fldGvJAPy32SlxbDh"
FLD_CLIENT_COI_CONTACT_EMAIL     = "fldvruOlAKMMMdsl8"
FLD_CLIENT_COI_CONTACT_ROLE      = "fldfE7GBbRJaTX2eh"

# Vendors (tblsOphSd5DKSZEro)
VENDORS_TABLE_ID = "tblsOphSd5DKSZEro"
FLD_VENDOR_NAME     = "fldb0BUb3wggDMJMp"
FLD_VENDOR_ALIASES  = "fldq07F1pGZjzefBk"

# Email Queue (tblCeRCf6RToTFkbL) — used fields
EMAIL_QUEUE_TABLE_ID = "tblCeRCf6RToTFkbL"

# Insurance Certificates (tbl0IH6zQQsXBff3l)
CERTS_TABLE_ID = "tbl0IH6zQQsXBff3l"

# ── Values ───────────────────────────────────────────────────────────────────

LADDER_TYPE_SHORT_DATED    = "Short-Dated Renewal"
LADDER_TYPE_NONCOMPLIANCE  = "Noncompliance Return"

STAGE_DAY_0_QUEUED      = "Day 0 Queued"
STAGE_DAY_0_SENT        = "Day 0 Sent"
STAGE_DAY_2_SENT        = "Day 2 Sent"
STAGE_DAY_4_SENT        = "Day 4 Sent"
STAGE_DAY_6_ESCALATED   = "Day 6 Escalated"
STAGE_RESPONDED         = "Responded"
STAGE_RESOLVED          = "Resolved"
STAGE_BOUNCED_ESCALATED = "Bounced-Escalated"
STAGE_CLOSED            = "Closed"

STATUS_ACTIVE   = "Active"
STATUS_TERMINAL = "Terminal"

DASHBOARD_APPROVAL_PENDING   = "Pending Approval"
DASHBOARD_APPROVAL_APPROVED  = "Approved & Sent"
DASHBOARD_APPROVAL_CANCELLED = "Cancelled"

# Email Queue Email Type tags used to wire sends back to ladders.
EQ_TYPE_DAY_0          = "Ladder Day 0"
EQ_TYPE_DAY_0_RETRY    = "Ladder Day 0 Retry"
EQ_TYPE_DAY_2          = "Ladder Day 2"
EQ_TYPE_DAY_4          = "Ladder Day 4"
EQ_TYPE_DAY_6_APPROVAL = "Ladder Day 6 Approval Request"

DAY_N_SENT_FIELD_BY_EQ_TYPE = {
    EQ_TYPE_DAY_0:       FLD_DAY_0_SENT_AT,
    EQ_TYPE_DAY_0_RETRY: None,   # retry does not re-stamp Day 0 Sent At
    EQ_TYPE_DAY_2:       FLD_DAY_2_SENT_AT,
    EQ_TYPE_DAY_4:       FLD_DAY_4_SENT_AT,
    EQ_TYPE_DAY_6_APPROVAL: None,
}

LADDER_ID_PREFIX_SHORT_DATED   = "LADDER-SHORT-DATED-"
LADDER_ID_PREFIX_NONCOMPLIANCE = "LADDER-NONCOMP-"

_COMPANY_SUFFIX_PATTERN = re.compile(
    r"\b(llc|l\.l\.c\.|inc|inc\.|incorporated|corporation|corp|corp\.|company|co|co\.|ltd|ltd\.)\b",
    flags=re.IGNORECASE,
)
_PUNCT_PATTERN = re.compile(r"[^\w\s]")
_WS_PATTERN = re.compile(r"\s+")

_REPLY_TO_IN_BODY = re.compile(
    r"(?:reply\s*to|please\s*reply\s*to|respond\s*to)[:\s]+([\w.+-]+@[\w-]+(?:\.[\w-]+)+)",
    flags=re.IGNORECASE,
)


# ── Dataclass for return values ──────────────────────────────────────────────

@dataclass
class LadderCreationResult:
    ladder_record_id: str
    ladder_id: str
    day_0_email_queue_id: Optional[str]


# ── API helpers ──────────────────────────────────────────────────────────────

def _api() -> Api:
    return Api(config.AIRTABLE_API_KEY)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_et() -> datetime:
    return datetime.now(ET)


# ── Normalization & matching ─────────────────────────────────────────────────

def normalize_for_matching(s: str) -> str:
    """Lowercase; strip LLC/Inc/Corp/Co/Company; strip punctuation; collapse whitespace."""
    if not s:
        return ""
    out = s.lower()
    out = _COMPANY_SUFFIX_PATTERN.sub(" ", out)
    out = _PUNCT_PATTERN.sub(" ", out)
    out = _WS_PATTERN.sub(" ", out).strip()
    return out


def _normalize_email(e: str) -> str:
    return (e or "").strip().lower()


def parse_body_reply_to(body_text: str) -> Optional[str]:
    """Extract 'reply to: foo@bar.com' style addresses from inbound email body."""
    if not body_text:
        return None
    m = _REPLY_TO_IN_BODY.search(body_text)
    if m:
        return _normalize_email(m.group(1))
    return None


# ── Ladder ID generation ─────────────────────────────────────────────────────

def generate_ladder_id(ladder_type: str, api: Api = None) -> str:
    """Generate next sequential ladder ID for this type.

    Queries existing Outbound Follow-Ups for the given type prefix,
    finds the maximum numeric suffix, returns max+1 zero-padded to 5 digits.
    """
    prefix = (
        LADDER_ID_PREFIX_SHORT_DATED
        if ladder_type == LADDER_TYPE_SHORT_DATED
        else LADDER_ID_PREFIX_NONCOMPLIANCE
    )
    api = api or _api()
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    try:
        formula = f"FIND('{prefix}', {{{FLD_LADDER_ID}}}) = 1"
        existing = table.all(formula=formula, fields=[FLD_LADDER_ID])
    except Exception as exc:
        logger.warning("Could not list existing ladder IDs: %s — starting at 1", exc)
        existing = []

    max_n = 0
    for row in existing:
        raw = row.get("fields", {}).get("Ladder ID") or row.get("fields", {}).get(FLD_LADDER_ID) or ""
        if not raw.startswith(prefix):
            continue
        try:
            n = int(raw[len(prefix):])
        except ValueError:
            continue
        if n > max_n:
            max_n = n

    return f"{prefix}{max_n + 1:05d}"


# ── Email Queue creation (internal) ──────────────────────────────────────────

def _write_ladder_email_queue_row(
    api: Api,
    ladder_record_id: str,
    primary_email: str,
    cc_emails: List[str],
    subject: str,
    body: str,
    eq_email_type: str,
    send_after_dt: datetime,
    vendor_id: Optional[str] = None,
    client_id: Optional[str] = None,
) -> Optional[str]:
    """Create one Email Queue row tagged to this ladder. Returns record id."""
    eq_table = api.table(config.AIRTABLE_BASE_ID, EMAIL_QUEUE_TABLE_ID)

    clean_cc = [e for e in (cc_emails or []) if e and "@" in e]

    fields = {
        "Primary Email": primary_email,
        "Subject": subject,
        "Body": body,
        "Email Type": eq_email_type,
        "Reminder Status": "Queued",
        "Record Status": "Active",
        "Send After": _iso_utc(send_after_dt),
        "Created At": _iso_utc(_now_et()),
        "Reminder Reason": f"LADDER:{ladder_record_id}",
    }
    if clean_cc:
        fields["CC Emails"] = clean_cc
    if vendor_id:
        fields["Vendor"] = [vendor_id]
    if client_id:
        fields["Client"] = [client_id]

    try:
        row = eq_table.create(fields, typecast=True)
        return row.get("id")
    except Exception as exc:
        logger.error("Failed to create ladder Email Queue row for %s: %s", ladder_record_id, exc)
        return None


def _append_eq_to_ladder(api: Api, ladder_record_id: str, new_eq_id: Optional[str]) -> None:
    if not new_eq_id:
        return
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)
    try:
        current = table.get(ladder_record_id)
        linked = current.get("fields", {}).get(FLD_LINKED_EQ_RECORDS) or current.get("fields", {}).get("Linked Email Queue Records") or []
        if not isinstance(linked, list):
            linked = []
        if new_eq_id not in linked:
            linked.append(new_eq_id)
        table.update(ladder_record_id, {FLD_LINKED_EQ_RECORDS: linked}, typecast=True)
    except Exception as exc:
        logger.warning("Could not append Email Queue link to ladder %s: %s", ladder_record_id, exc)


# ── create_ladder ────────────────────────────────────────────────────────────

def create_ladder(
    cert_id: Optional[str],
    ladder_type: str,
    email_subject: str,
    email_body: str,
    original_sender: str,
    vendor_id: Optional[str] = None,
    client_id: Optional[str] = None,
    named_insured: str = "",
    body_reply_to: Optional[str] = None,
    pdf_contact_email: Optional[str] = None,
    trigger_context: str = "",
    api: Api = None,
) -> Optional[LadderCreationResult]:
    """Create an Outbound Follow-Up record, compute cadence times, queue Day 0 email.

    Returns LadderCreationResult(ladder_record_id, ladder_id, day_0_email_queue_id)
    or None if creation failed.
    """
    if not original_sender or "@" not in original_sender:
        logger.warning("create_ladder aborted — no valid Original Sender (cert=%s type=%s)", cert_id, ladder_type)
        return None

    api = api or _api()
    ladder_table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    original_sender = _normalize_email(original_sender)
    body_reply_to = _normalize_email(body_reply_to) if body_reply_to else None
    pdf_contact_email = _normalize_email(pdf_contact_email) if pdf_contact_email else None

    ladder_id = generate_ladder_id(ladder_type, api=api)

    # Day 0 send time (immediate-send rule)
    day_0_send_at = send_now_or_defer()

    # Cadence times — anchored from day_0_send_at
    day_2_send_at = add_business_days(day_0_send_at, 2)
    day_4_send_at = add_business_days(day_0_send_at, 4)
    day_6_escalate_at = add_business_days(day_0_send_at, 6)

    fields = {
        FLD_LADDER_ID:          ladder_id,
        FLD_LADDER_TYPE:        ladder_type,
        FLD_STAGE:              STAGE_DAY_0_QUEUED,
        FLD_STATUS:             STATUS_ACTIVE,
        FLD_NAMED_INSURED:      named_insured or "",
        FLD_ORIGINAL_SENDER:    original_sender,
        FLD_DAY_2_SEND_AT:      _iso_utc(day_2_send_at),
        FLD_DAY_4_SEND_AT:      _iso_utc(day_4_send_at),
        FLD_DAY_6_ESCALATE_AT:  _iso_utc(day_6_escalate_at),
        FLD_EMAIL_SUBJECT:      email_subject,
        FLD_EMAIL_BODY:         email_body,
        FLD_BOUNCE_COUNT:       0,
        FLD_CREATED_AT:         _iso_utc(_now_et()),
    }
    if body_reply_to:
        fields[FLD_BODY_REPLY_TO] = body_reply_to
    if pdf_contact_email:
        fields[FLD_PDF_CONTACT_EMAIL] = pdf_contact_email
    if vendor_id:
        fields[FLD_VENDOR_LINK] = [vendor_id]
    if client_id:
        fields[FLD_CLIENT_LINK] = [client_id]
    if cert_id:
        fields[FLD_CERTIFICATE_LINK] = [cert_id]

    try:
        ladder_row = ladder_table.create(fields, typecast=True)
    except Exception as exc:
        logger.error("Failed to create ladder %s: %s", ladder_id, exc)
        return None

    ladder_record_id = ladder_row.get("id")
    logger.info("Ladder created: %s (%s) cert=%s vendor=%s", ladder_id, ladder_type, cert_id, vendor_id)

    # Queue Day 0 email — primary only (no CC on Day 0 per spec)
    eq_id = _write_ladder_email_queue_row(
        api=api,
        ladder_record_id=ladder_record_id,
        primary_email=original_sender,
        cc_emails=[body_reply_to] if body_reply_to else [],
        subject=email_subject,
        body=email_body,
        eq_email_type=EQ_TYPE_DAY_0,
        send_after_dt=day_0_send_at,
        vendor_id=vendor_id,
        client_id=client_id,
    )
    _append_eq_to_ladder(api, ladder_record_id, eq_id)

    return LadderCreationResult(
        ladder_record_id=ladder_record_id,
        ladder_id=ladder_id,
        day_0_email_queue_id=eq_id,
    )


# ── terminate_ladder ─────────────────────────────────────────────────────────

def terminate_ladder(
    ladder_record_id: str,
    new_stage: str,
    reason: str,
    api: Api = None,
    extra_fields: Optional[dict] = None,
) -> bool:
    api = api or _api()
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)
    fields = {
        FLD_STAGE: new_stage,
        FLD_STATUS: STATUS_TERMINAL,
        FLD_TERMINATION_REASON: reason,
    }
    if extra_fields:
        fields.update(extra_fields)
    try:
        table.update(ladder_record_id, fields, typecast=True)
        logger.info("Ladder %s terminated: stage=%s reason=%s", ladder_record_id, new_stage, reason)
        return True
    except Exception as exc:
        logger.error("Failed to terminate ladder %s: %s", ladder_record_id, exc)
        return False


# ── advance_ladder (called by cron) ──────────────────────────────────────────

def advance_ladder(ladder_record_id: str, api: Api = None, now_et: datetime = None) -> bool:
    """Advance the ladder one stage if the scheduled time has passed.

    Returns True if an action was taken.
    """
    api = api or _api()
    now_et = now_et or _now_et()
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    try:
        row = table.get(ladder_record_id)
    except Exception as exc:
        logger.error("advance_ladder: can't fetch %s: %s", ladder_record_id, exc)
        return False

    f = row.get("fields", {})
    if (f.get("Status") or f.get(FLD_STATUS)) != STATUS_ACTIVE:
        return False

    stage = f.get("Stage") or f.get(FLD_STAGE)

    day_0_sent = _parse_iso(f.get("Day 0 Sent At") or f.get(FLD_DAY_0_SENT_AT))
    day_2_send = _parse_iso(f.get("Day 2 Send At") or f.get(FLD_DAY_2_SEND_AT))
    day_4_send = _parse_iso(f.get("Day 4 Send At") or f.get(FLD_DAY_4_SEND_AT))
    day_6_esc  = _parse_iso(f.get("Day 6 Escalate At") or f.get(FLD_DAY_6_ESCALATE_AT))

    original_sender   = f.get("Original Sender") or f.get(FLD_ORIGINAL_SENDER) or ""
    body_reply_to     = f.get("Body Reply-To") or f.get(FLD_BODY_REPLY_TO) or ""
    pdf_contact_email = f.get("PDF Contact Email") or f.get(FLD_PDF_CONTACT_EMAIL) or ""

    email_subject = f.get("Email Subject") or f.get(FLD_EMAIL_SUBJECT) or ""
    email_body    = f.get("Email Body") or f.get(FLD_EMAIL_BODY) or ""

    vendor_links = f.get("Vendor Link") or f.get(FLD_VENDOR_LINK) or []
    client_links = f.get("Client Link") or f.get(FLD_CLIENT_LINK) or []
    vendor_id = vendor_links[0] if vendor_links else None
    client_id = client_links[0] if client_links else None

    # Day 2
    if stage == STAGE_DAY_0_SENT and day_2_send and now_et >= day_2_send:
        cc = [e for e in (body_reply_to, pdf_contact_email) if e and e != original_sender]
        body = _prepend_followup_header(email_body)
        eq_id = _write_ladder_email_queue_row(
            api, ladder_record_id, original_sender, cc, email_subject, body,
            EQ_TYPE_DAY_2, now_et, vendor_id, client_id,
        )
        _append_eq_to_ladder(api, ladder_record_id, eq_id)
        logger.info("Ladder %s advanced to Day 2 — queued EQ %s", ladder_record_id, eq_id)
        return True

    # Day 4
    if stage == STAGE_DAY_2_SENT and day_4_send and now_et >= day_4_send:
        cc = [e for e in (body_reply_to, pdf_contact_email) if e and e != original_sender]
        body = _prepend_followup_header(email_body)
        eq_id = _write_ladder_email_queue_row(
            api, ladder_record_id, original_sender, cc, email_subject, body,
            EQ_TYPE_DAY_4, now_et, vendor_id, client_id,
        )
        _append_eq_to_ladder(api, ladder_record_id, eq_id)
        logger.info("Ladder %s advanced to Day 4 — queued EQ %s", ladder_record_id, eq_id)
        return True

    # Day 6 — escalate to Haley, no vendor-facing email
    if stage == STAGE_DAY_4_SENT and day_6_esc and now_et >= day_6_esc:
        _escalate_to_day_6(api, ladder_record_id, f, now_et)
        return True

    return False


def _prepend_followup_header(body: str) -> str:
    return (
        "Following up on the below — let me know if you need anything from us.\n\n"
        "---\n\n"
        + (body or "")
    )


def _parse_iso(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Airtable returns "2025-03-03T13:00:00.000Z" or with offset
        s = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(ET)
    except (ValueError, TypeError):
        return None


# ── Day 6 escalation (interim: Haley-facing approval email) ──────────────────

def _escalate_to_day_6(api: Api, ladder_record_id: str, ladder_fields: dict, now_et: datetime) -> None:
    """Set Day 6 state + queue Haley-approval email.

    TODO(future): replace the approval email below with an entry in the
    Pending Escalations view on the localhost:8099 internal review dashboard.
    That dashboard lives in a Cowork output folder outside this repo and is
    out of scope for this build. See delivery report.
    """
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    vendor_links = ladder_fields.get("Vendor Link") or ladder_fields.get(FLD_VENDOR_LINK) or []
    client_links = ladder_fields.get("Client Link") or ladder_fields.get(FLD_CLIENT_LINK) or []
    vendor_id = vendor_links[0] if vendor_links else None
    client_id = client_links[0] if client_links else None

    vendor_name = _lookup_vendor_name(api, vendor_id) if vendor_id else "the vendor"
    client = _lookup_client_record(api, client_id) if client_id else {}

    # Resolve Day 6 recipients (see brief: COI Contact first, Primary Contact as CC/fallback)
    primary, cc = _resolve_day_6_recipients(client)

    day_0_sent_at = _parse_iso(ladder_fields.get("Day 0 Sent At") or ladder_fields.get(FLD_DAY_0_SENT_AT))
    day_2_sent_at = _parse_iso(ladder_fields.get("Day 2 Sent At") or ladder_fields.get(FLD_DAY_2_SENT_AT))
    day_4_sent_at = _parse_iso(ladder_fields.get("Day 4 Sent At") or ladder_fields.get(FLD_DAY_4_SENT_AT))

    day_0_date = _fmt_date(day_0_sent_at)
    day_2_date = _fmt_date(day_2_sent_at)
    day_4_date = _fmt_date(day_4_sent_at)

    gc_first_name = _first_name(client.get(FLD_CLIENT_COI_CONTACT_NAME)) \
                    or _first_name(client.get(FLD_CLIENT_PRIMARY_CONTACT_NAME)) \
                    or "there"

    cert_expiration = _describe_cert_expiration(api, ladder_fields)

    draft_subject = f"Action needed — {vendor_name} renewal documentation"
    draft_body = (
        f"Hi {gc_first_name},\n\n"
        f"Wanted to flag that {vendor_name}'s current certificate shows coverage expiring "
        f"{cert_expiration}. We've reached out to them/their agent three times at "
        f"{day_0_date}, {day_2_date}, {day_4_date} asking for renewal documentation, "
        f"and haven't received a reply.\n\n"
        f"Can you reach out on your end to request the renewal certificate? If you "
        f"receive a COI back, please forward it to coi@carolinacompliancesolutions.com "
        f"so we have it stored and your file stays current.\n\n"
        f"Thanks,\n"
        f"Haley\n"
        f"Carolina Compliance Solutions"
    )

    # Update ladder state first — even if the approval email fails to queue,
    # the ladder is correctly marked as escalated.
    try:
        table.update(ladder_record_id, {
            FLD_STAGE: STAGE_DAY_6_ESCALATED,
            FLD_STATUS: STATUS_ACTIVE,   # stays Active until Haley approves or cancels
            FLD_DASHBOARD_ESCALATED: _iso_utc(now_et),
            FLD_DASHBOARD_APPROVAL: DASHBOARD_APPROVAL_PENDING,
        }, typecast=True)
    except Exception as exc:
        logger.error("Failed to set Day 6 state on ladder %s: %s", ladder_record_id, exc)
        return

    # Queue the approval email to Haley (INTERIM for dashboard — see TODO above).
    haley_body = (
        "A follow-up ladder has reached Day 6 with no reply from the vendor or their agent.\n\n"
        "Below is the pre-drafted GC-facing email. Review, adjust if needed, and send from "
        "your own mailbox. (Reply-to-approve is not yet wired up; the Pending Escalations "
        "dashboard view will replace this interim flow.)\n\n"
        f"Ladder record: {ladder_fields.get('Ladder ID') or ladder_fields.get(FLD_LADDER_ID) or ladder_record_id}\n"
        f"Recipient (primary): {primary or '(unresolved — check Client record)'}\n"
        f"Recipient (CC): {', '.join(cc) if cc else '(none)'}\n\n"
        "──── DRAFT SUBJECT ────\n"
        f"{draft_subject}\n\n"
        "──── DRAFT BODY ────\n"
        f"{draft_body}\n"
    )

    approval_eq_id = _write_ladder_email_queue_row(
        api=api,
        ladder_record_id=ladder_record_id,
        primary_email=config.OWNER_EMAIL,
        cc_emails=[],
        subject=f"[APPROVAL NEEDED] {draft_subject}",
        body=haley_body,
        eq_email_type=EQ_TYPE_DAY_6_APPROVAL,
        send_after_dt=now_et,
        vendor_id=vendor_id,
        client_id=client_id,
    )
    _append_eq_to_ladder(api, ladder_record_id, approval_eq_id)

    logger.info("Ladder %s escalated to Day 6 — approval email queued to %s", ladder_record_id, config.OWNER_EMAIL)


def _resolve_day_6_recipients(client_fields: dict) -> tuple[str, List[str]]:
    coi_email = (client_fields.get(FLD_CLIENT_COI_CONTACT_EMAIL) or "").strip()
    primary_email = (client_fields.get(FLD_CLIENT_PRIMARY_CONTACT_EMAIL) or "").strip()
    if coi_email:
        return coi_email, ([primary_email] if primary_email and primary_email != coi_email else [])
    return primary_email, []


def _first_name(full: str) -> str:
    if not full:
        return ""
    return full.strip().split(" ")[0]


def _fmt_date(dt: Optional[datetime]) -> str:
    if not dt:
        return "(unknown)"
    return dt.astimezone(ET).strftime("%b %-d")


def _describe_cert_expiration(api: Api, ladder_fields: dict) -> str:
    cert_links = ladder_fields.get("Certificate Link") or ladder_fields.get(FLD_CERTIFICATE_LINK) or []
    if not cert_links:
        return "soon"
    try:
        certs_t = api.table(config.AIRTABLE_BASE_ID, CERTS_TABLE_ID)
        cert = certs_t.get(cert_links[0])
        exp = cert.get("fields", {}).get("Expiration Date") or ""
        if exp:
            return f"on {exp}"
    except Exception:
        pass
    return "soon"


def _lookup_vendor_name(api: Api, vendor_id: str) -> str:
    try:
        t = api.table(config.AIRTABLE_BASE_ID, VENDORS_TABLE_ID)
        row = t.get(vendor_id)
        return row.get("fields", {}).get("Vendor Name") or row.get("fields", {}).get(FLD_VENDOR_NAME) or ""
    except Exception:
        return ""


def _lookup_client_record(api: Api, client_id: str) -> dict:
    try:
        t = api.table(config.AIRTABLE_BASE_ID, CLIENTS_TABLE_ID)
        row = t.get(client_id)
        return row.get("fields", {}) or {}
    except Exception:
        return {}


# ── Response matching ────────────────────────────────────────────────────────

def check_response_match(
    inbound_sender: str,
    inbound_subject: str,
    inbound_body: str,
    api: Api = None,
) -> Optional[str]:
    """If an inbound email matches exactly one Active ladder by sender + named insured,
    return that ladder's record id. Otherwise return None.
    """
    api = api or _api()
    sender = _normalize_email(inbound_sender)
    if not sender or "@" not in sender:
        return None

    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    try:
        safe = sender.replace("'", "\\'")
        formula = (
            f"AND({{{FLD_STATUS}}} = '{STATUS_ACTIVE}', "
            f"OR({{{FLD_ORIGINAL_SENDER}}} = '{safe}', "
            f"{{{FLD_BODY_REPLY_TO}}} = '{safe}', "
            f"{{{FLD_PDF_CONTACT_EMAIL}}} = '{safe}'))"
        )
        candidates = table.all(formula=formula)
    except Exception as exc:
        logger.warning("check_response_match: ladder query failed: %s", exc)
        return None

    if not candidates:
        return None

    haystack = normalize_for_matching((inbound_subject or "") + " " + (inbound_body or "")[:1000])

    matches = []
    for row in candidates:
        f = row.get("fields", {})
        ladder_record_id = row["id"]
        named_insured = f.get("Named Insured Snapshot") or f.get(FLD_NAMED_INSURED) or ""
        forms = [normalize_for_matching(named_insured)]

        vendor_links = f.get("Vendor Link") or f.get(FLD_VENDOR_LINK) or []
        if vendor_links:
            aliases = _lookup_vendor_aliases(api, vendor_links[0])
            forms.extend(normalize_for_matching(a) for a in aliases)

        for form in forms:
            if form and form in haystack:
                matches.append((ladder_record_id, f.get("Created At") or f.get(FLD_CREATED_AT) or ""))
                break

    if not matches:
        logger.info("Inbound from %s — no named-insured match on %d ladder candidate(s); not halted",
                    sender, len(candidates))
        return None

    if len(matches) == 1:
        return matches[0][0]

    matches.sort(key=lambda x: x[1] or "")
    logger.warning("Inbound from %s matched %d active ladders — halting oldest (%s)",
                   sender, len(matches), matches[0][0])
    return matches[0][0]


def _lookup_vendor_aliases(api: Api, vendor_id: str) -> List[str]:
    try:
        t = api.table(config.AIRTABLE_BASE_ID, VENDORS_TABLE_ID)
        row = t.get(vendor_id)
        raw = row.get("fields", {}).get("Aliases") or row.get("fields", {}).get(FLD_VENDOR_ALIASES) or ""
    except Exception:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if x]
    return [line.strip() for line in str(raw).splitlines() if line.strip()]


# ── Vendor-level halt (compliant cert arrival) ───────────────────────────────

def halt_ladders_for_vendor(vendor_id: str, reason: str, api: Api = None) -> int:
    """Terminate all Active ladders linked to this vendor as Resolved."""
    if not vendor_id:
        return 0
    api = api or _api()
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    try:
        safe = vendor_id.replace("'", "\\'")
        formula = (
            f"AND({{{FLD_STATUS}}} = '{STATUS_ACTIVE}', "
            f"FIND('{safe}', ARRAYJOIN({{{FLD_VENDOR_LINK}}})))"
        )
        rows = table.all(formula=formula)
    except Exception as exc:
        logger.warning("halt_ladders_for_vendor: query failed for %s: %s", vendor_id, exc)
        return 0

    halted = 0
    for row in rows:
        if terminate_ladder(row["id"], STAGE_RESOLVED, reason, api=api):
            halted += 1
    if halted:
        logger.info("Halted %d ladder(s) for vendor %s: %s", halted, vendor_id, reason)
    return halted


# ── Bounce handling ──────────────────────────────────────────────────────────

def handle_bounce(
    bounced_address: str,
    api: Api = None,
    email_queue_id: Optional[str] = None,
) -> Optional[str]:
    """When a bounce is received, find the ladder that sent to that address
    and either schedule a retry (Day 0 → PDF contact) or escalate.

    Returns ladder_record_id if an action was taken, else None.
    """
    api = api or _api()
    addr = _normalize_email(bounced_address)
    if not addr:
        return None

    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    try:
        safe = addr.replace("'", "\\'")
        formula = (
            f"AND({{{FLD_STATUS}}} = '{STATUS_ACTIVE}', "
            f"OR({{{FLD_ORIGINAL_SENDER}}} = '{safe}', "
            f"{{{FLD_PDF_CONTACT_EMAIL}}} = '{safe}', "
            f"{{{FLD_BODY_REPLY_TO}}} = '{safe}'))"
        )
        candidates = table.all(formula=formula)
    except Exception as exc:
        logger.warning("handle_bounce: query failed for %s: %s", addr, exc)
        return None

    if not candidates:
        return None

    # Pick the most-recently-created candidate (most likely the bounce source).
    candidates.sort(
        key=lambda r: r.get("fields", {}).get("Created At") or r.get("fields", {}).get(FLD_CREATED_AT) or "",
        reverse=True,
    )
    ladder = candidates[0]
    ladder_record_id = ladder["id"]
    f = ladder.get("fields", {})
    stage = f.get("Stage") or f.get(FLD_STAGE)
    original_sender   = f.get("Original Sender") or f.get(FLD_ORIGINAL_SENDER) or ""
    pdf_contact_email = f.get("PDF Contact Email") or f.get(FLD_PDF_CONTACT_EMAIL) or ""
    bounce_count = int(f.get("Bounce Count") or f.get(FLD_BOUNCE_COUNT) or 0)
    email_subject = f.get("Email Subject") or f.get(FLD_EMAIL_SUBJECT) or ""
    email_body    = f.get("Email Body") or f.get(FLD_EMAIL_BODY) or ""
    vendor_links  = f.get("Vendor Link") or f.get(FLD_VENDOR_LINK) or []
    client_links  = f.get("Client Link") or f.get(FLD_CLIENT_LINK) or []
    vendor_id = vendor_links[0] if vendor_links else None
    client_id = client_links[0] if client_links else None

    new_bounce_count = bounce_count + 1
    try:
        table.update(ladder_record_id, {FLD_BOUNCE_COUNT: new_bounce_count}, typecast=True)
    except Exception as exc:
        logger.warning("handle_bounce: could not increment Bounce Count on %s: %s", ladder_record_id, exc)

    # Escalation triggers:
    # 1. Second+ bounce on this ladder.
    # 2. The bounce is on the PDF contact (no fallback recipient left).
    if new_bounce_count >= 2 or addr == _normalize_email(pdf_contact_email):
        terminate_ladder(
            ladder_record_id,
            STAGE_BOUNCED_ESCALATED,
            f"Bounced: {addr} (bounce_count={new_bounce_count})",
            api=api,
            extra_fields={
                FLD_DASHBOARD_APPROVAL:   DASHBOARD_APPROVAL_PENDING,
                FLD_DASHBOARD_ESCALATED:  _iso_utc(_now_et()),
            },
        )
        return ladder_record_id

    # First bounce on Day 0 original sender → retry to PDF contact.
    if stage in (STAGE_DAY_0_QUEUED, STAGE_DAY_0_SENT) and addr == _normalize_email(original_sender):
        if not pdf_contact_email:
            terminate_ladder(
                ladder_record_id,
                STAGE_BOUNCED_ESCALATED,
                f"Bounced: {addr} and no PDF contact on file",
                api=api,
                extra_fields={
                    FLD_DASHBOARD_APPROVAL:   DASHBOARD_APPROVAL_PENDING,
                    FLD_DASHBOARD_ESCALATED:  _iso_utc(_now_et()),
                },
            )
            return ladder_record_id

        retry_time = send_now_or_defer(_now_et())
        eq_id = _write_ladder_email_queue_row(
            api=api,
            ladder_record_id=ladder_record_id,
            primary_email=pdf_contact_email,
            cc_emails=[],
            subject=email_subject,
            body=email_body,
            eq_email_type=EQ_TYPE_DAY_0_RETRY,
            send_after_dt=retry_time,
            vendor_id=vendor_id,
            client_id=client_id,
        )
        _append_eq_to_ladder(api, ladder_record_id, eq_id)
        logger.info("Ladder %s — bounce retry queued to PDF contact %s (EQ %s)",
                    ladder_record_id, pdf_contact_email, eq_id)
        return ladder_record_id

    # Fallback: if bounce is on Day 2/4 but not at escalation threshold yet,
    # leave ladder running. At least one other recipient delivered.
    logger.info("Ladder %s — bounce on %s at stage %s; no action (bounce_count=%d)",
                ladder_record_id, addr, stage, new_bounce_count)
    return ladder_record_id


# ── Hook used by module_10 after successful send ─────────────────────────────

def record_send_success(
    ladder_record_id: str,
    eq_email_type: str,
    sent_at: datetime = None,
    api: Api = None,
) -> None:
    """Stamp the appropriate Day N Sent At + advance Stage when module_10 reports
    a successful send.
    """
    api = api or _api()
    sent_at = sent_at or _now_et()
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    day_field = DAY_N_SENT_FIELD_BY_EQ_TYPE.get(eq_email_type)
    updates = {}

    if day_field:
        updates[day_field] = _iso_utc(sent_at)

    stage_transition = {
        EQ_TYPE_DAY_0: STAGE_DAY_0_SENT,
        EQ_TYPE_DAY_2: STAGE_DAY_2_SENT,
        EQ_TYPE_DAY_4: STAGE_DAY_4_SENT,
    }.get(eq_email_type)
    if stage_transition:
        updates[FLD_STAGE] = stage_transition

    if not updates:
        return

    try:
        table.update(ladder_record_id, updates, typecast=True)
        logger.info("Ladder %s updated on send: %s", ladder_record_id, updates)
    except Exception as exc:
        logger.error("Failed to update ladder %s after send: %s", ladder_record_id, exc)
