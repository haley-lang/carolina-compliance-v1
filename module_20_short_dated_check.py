"""Module 20 — Short-Dated Renewal Check.

Called at the end of module_7b (compliance evaluation) for every certificate
processed, regardless of compliance status. A cert can be compliant AND
short-dated — these are independent concerns.

Logic:
1. Fetch the certificate and its linked Insurance Policies.
2. Filter to policies where Days Until Expiration formula is in (0, 30].
3. Enforce 14-day cooldown via Short-Dated Flagged At.
4. Stamp Has Short-Dated Policies + Short-Dated Flagged At on the cert.
5. Build the short-dated renewal email and call module_22.create_ladder.

Legal framing (locked): the email MUST describe "coverage shown on this
certificate" and "documentation gap" — never "your insurance expires" or
"policy expires." CCS tracks documentation, not insurance coverage.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import List, Optional

from pyairtable import Api

import config
from module_22_followup_ladder import (
    create_ladder,
    LADDER_TYPE_SHORT_DATED,
)


logger = logging.getLogger(__name__)


# ── Airtable IDs ─────────────────────────────────────────────────────────────

# Insurance Certificates (tbl0IH6zQQsXBff3l)
CERTS_TABLE_ID = "tbl0IH6zQQsXBff3l"
FLD_CERT_SHORT_DATED_FLAGGED_AT = "fldMapA69CLUWH2Y3"
FLD_CERT_HAS_SHORT_DATED        = "fldfmNSxUtTfO2TML"
FLD_CERT_NAMED_INSURED_LINK     = None   # not used; resolved via vendor link
# Certificate link field names (both field ID and human name get used across codebase)
FLD_CERT_VENDOR_LINK            = "fldVendorLinkPlaceholder"   # resolved by name in code
FLD_CERT_POLICIES_LINK          = "fldInsurancePoliciesPlaceholder"

# Insurance Policies (tblpPcmm5ANE0bMNB)
POLICIES_TABLE_ID = "tblpPcmm5ANE0bMNB"
FLD_POLICY_DAYS_UNTIL_EXP = "fldIEJftKSxsPGSMI"

# Incoming Extractions (tblT88Ty6d6M766oY)
EXTRACTIONS_TABLE_ID = "tblT88Ty6d6M766oY"
FLD_EXT_CONTACT_EMAILS = "fldzuzzd0hlvuVRN6"

# Clients (tbltnBIWke20IEI3K)
CLIENTS_TABLE_ID = "tbltnBIWke20IEI3K"

# Vendors (tblsOphSd5DKSZEro)
VENDORS_TABLE_ID = "tblsOphSd5DKSZEro"


COOLDOWN_DAYS = 14


# ── Main entry ───────────────────────────────────────────────────────────────

def check_and_trigger_short_dated(cert_id: str, api: Api = None) -> bool:
    """Returns True if a ladder was created for this cert."""
    if not cert_id:
        return False
    api = api or Api(config.AIRTABLE_API_KEY)

    certs_t = api.table(config.AIRTABLE_BASE_ID, CERTS_TABLE_ID)
    try:
        cert = certs_t.get(cert_id)
    except Exception as exc:
        logger.warning("short_dated: can't fetch cert %s: %s", cert_id, exc)
        return False

    cert_fields = cert.get("fields", {})

    if _in_cooldown(cert_fields):
        logger.info("short_dated: cert %s within 14-day cooldown — skip", cert_id)
        return False

    policy_ids = _extract_linked_policy_ids(cert_fields)
    if not policy_ids:
        return False

    short_dated = _fetch_short_dated_policies(api, policy_ids)
    if not short_dated:
        return False

    # Resolve vendor, client, named insured
    vendor_id = _extract_first_linked(cert_fields, "Vendor Link") or _extract_first_linked(cert_fields, "Vendor")
    client_id = _extract_first_linked(cert_fields, "Client Link") or _extract_first_linked(cert_fields, "Client")
    named_insured = (cert_fields.get("Named Insured") or "").strip()
    gc_name = _lookup_client_name(api, client_id) if client_id else ""

    # Resolve Original Sender + PDF Contact from linked Incoming Extraction.
    original_sender, pdf_contact = _resolve_contacts(api, cert_fields, named_insured)
    if not original_sender:
        logger.warning("short_dated: cert %s has no resolvable sender email — skip", cert_id)
        return False

    subject = "Renewal certificate needed — coverage shown on current certificate expires within 30 days"
    body = _build_short_dated_body(named_insured or "your company", gc_name or "the general contractor", short_dated)

    result = create_ladder(
        cert_id=cert_id,
        ladder_type=LADDER_TYPE_SHORT_DATED,
        email_subject=subject,
        email_body=body,
        original_sender=original_sender,
        pdf_contact_email=pdf_contact,
        vendor_id=vendor_id,
        client_id=client_id,
        named_insured=named_insured,
        trigger_context=f"Short-dated cert {cert_id}: {len(short_dated)} policy/policies ≤30 days",
        api=api,
    )
    if not result:
        return False

    # Stamp the cert after the ladder is committed.
    try:
        certs_t.update(cert_id, {
            FLD_CERT_HAS_SHORT_DATED: True,
            FLD_CERT_SHORT_DATED_FLAGGED_AT: date.today().isoformat(),
        }, typecast=True)
    except Exception as exc:
        logger.warning("short_dated: ladder %s created but cert stamp failed: %s", result.ladder_id, exc)

    logger.info("short_dated: ladder %s created for cert %s (%d short-dated policy/policies)",
                result.ladder_id, cert_id, len(short_dated))
    return True


# ── Helpers ──────────────────────────────────────────────────────────────────

def _in_cooldown(cert_fields: dict) -> bool:
    raw = cert_fields.get("Short-Dated Flagged At") or cert_fields.get(FLD_CERT_SHORT_DATED_FLAGGED_AT)
    if not raw:
        return False
    try:
        flagged = date.fromisoformat(str(raw)[:10])
    except ValueError:
        return False
    return (date.today() - flagged) < timedelta(days=COOLDOWN_DAYS)


def _extract_linked_policy_ids(cert_fields: dict) -> List[str]:
    for key in ("Insurance Policies", "Policies", "Policy Link"):
        ids = cert_fields.get(key)
        if isinstance(ids, list) and ids:
            return ids
    return []


def _extract_first_linked(fields: dict, key: str) -> Optional[str]:
    v = fields.get(key)
    if isinstance(v, list) and v:
        return v[0]
    return None


def _fetch_short_dated_policies(api: Api, policy_ids: List[str]) -> List[dict]:
    """Return only policies with 0 < Days Until Expiration <= 30."""
    policies_t = api.table(config.AIRTABLE_BASE_ID, POLICIES_TABLE_ID)
    short = []
    for pid in policy_ids:
        try:
            row = policies_t.get(pid)
        except Exception:
            continue
        pf = row.get("fields", {})
        days = pf.get("Days Until Expiration")
        if days is None:
            days = pf.get(FLD_POLICY_DAYS_UNTIL_EXP)
        try:
            d = int(days)
        except (TypeError, ValueError):
            continue
        if 0 < d <= 30:
            short.append(row)
    return short


def _resolve_contacts(api: Api, cert_fields: dict, named_insured: str) -> tuple[str, str]:
    """Resolve Original Sender and PDF Contact from the cert's linked Incoming Extraction.

    Returns (original_sender, pdf_contact). Either may be empty string.
    """
    extraction_ids = cert_fields.get("Incoming Extractions") or cert_fields.get("Extraction Link") or []
    if not isinstance(extraction_ids, list):
        extraction_ids = [extraction_ids] if extraction_ids else []

    emails: List[str] = []
    if extraction_ids:
        ext_t = api.table(config.AIRTABLE_BASE_ID, EXTRACTIONS_TABLE_ID)
        for eid in extraction_ids:
            try:
                ext = ext_t.get(eid)
            except Exception:
                continue
            raw = ext.get("fields", {}).get("Contact Emails") or ext.get("fields", {}).get(FLD_EXT_CONTACT_EMAILS) or ""
            if isinstance(raw, list):
                emails.extend(str(e).strip().lower() for e in raw if e)
            else:
                emails.extend(e.strip().lower() for e in str(raw).replace(",", "\n").splitlines() if e.strip())

    # Deduplicate preserving order
    seen = set()
    emails = [e for e in emails if e and "@" in e and not (e in seen or seen.add(e))]

    if not emails:
        return "", ""
    if len(emails) == 1:
        return emails[0], emails[0]
    return emails[0], emails[1]


def _lookup_client_name(api: Api, client_id: str) -> str:
    try:
        t = api.table(config.AIRTABLE_BASE_ID, CLIENTS_TABLE_ID)
        row = t.get(client_id)
        return (row.get("fields", {}).get("Client Name") or "").strip()
    except Exception:
        return ""


def _build_short_dated_body(named_insured: str, gc_name: str, short_dated_policies: List[dict]) -> str:
    """Short-dated renewal email body. Language is locked — do not alter.

    Legal framing: describe "coverage shown on this certificate" and
    "documentation gap" — never "your insurance expires" or "policy expires".
    """
    bullet_lines = []
    for p in short_dated_policies:
        pf = p.get("fields", {})
        ptype = pf.get("Policy Type") or "Policy"
        pnum = pf.get("Policy Number") or ""
        exp = pf.get("Expiration Date") or ""
        exp_fmt = _format_date_month_d_yyyy(exp) if exp else "(date not on file)"
        num_part = f" — Policy #{pnum}" if pnum else ""
        bullet_lines.append(f"- {ptype}{num_part} — expires {exp_fmt}")

    bullets = "\n".join(bullet_lines) if bullet_lines else "- (policy details unavailable)"

    return (
        "Hi,\n\n"
        f"Thanks for submitting the certificate for {named_insured}. We've logged it on file for {gc_name}.\n\n"
        "Wanted to flag that the following coverage shown on this certificate expires within the next 30 days:\n\n"
        f"{bullets}\n\n"
        f"Could you send the renewal certificate when it's available? Sending it now helps keep {gc_name}'s "
        "file current without a documentation gap.\n\n"
        "Thanks,\n"
        "Carolina Compliance Solutions"
    )


def _format_date_month_d_yyyy(s: str) -> str:
    try:
        d = date.fromisoformat(str(s)[:10])
    except ValueError:
        return str(s)
    return d.strftime("%B %-d, %Y")
