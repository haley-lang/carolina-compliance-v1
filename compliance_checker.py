"""
Module 5 — Compliance Checker
For every Vendor, evaluates linked Insurance Policies and sets
"Compliance Status" to one of: Compliant | Expiring Soon | Expired | Needs Review.

Rules (evaluated in priority order):
  1. No policies linked          → Needs Review
  2. Any policy missing Expiration Date → Needs Review
  3. Any policy expired (before today)  → Expired
  4. Any policy expiring within 30 days → Expiring Soon
  5. All policies current               → Compliant
"""

import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

TABLE_VENDORS  = "Vendors"
TABLE_POLICIES = "Insurance Policies"

EXPIRY_WARNING_DAYS = 30

# Compliance status values (must match Airtable single-select options exactly)
STATUS_COMPLIANT      = "Compliant"
STATUS_EXPIRING_SOON  = "Expiring Soon"
STATUS_EXPIRED        = "Expired"
STATUS_NEEDS_REVIEW   = "Needs Review"

# Date formats Module 4 writes (MM/DD/YYYY from extraction + ISO fallback)
_DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_base_id(raw: str) -> str:
    """Extract just the appXXXXXXXXXX portion from AIRTABLE_BASE_ID."""
    match = re.search(r"(app[A-Za-z0-9]{10,})", raw)
    if not match:
        raise ValueError(
            f"AIRTABLE_BASE_ID does not look like a valid base ID: '{raw}'. "
            "It should start with 'app' (e.g. appCGgww0Pt7KE04u)."
        )
    return match.group(1)


def parse_date(raw: str) -> Optional[date]:
    """Try multiple date formats; return a date object or None if unparseable."""
    cleaned = raw.strip() if raw else ""
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    logger.warning("Could not parse date value: '%s'", cleaned)
    return None


def evaluate_compliance(policies: List[dict], today: date) -> Tuple[str, str]:
    """
    Evaluate a list of policy field-dicts and return (status, reason).
    Policies is a list of Airtable record["fields"] dicts.
    """
    if not policies:
        return STATUS_NEEDS_REVIEW, "no policies linked"

    worst = STATUS_COMPLIANT
    reason = "all policies current"

    for policy in policies:
        policy_num  = policy.get("Policy Number") or "(no number)"
        raw_expiry  = policy.get("Expiration Date") or ""

        if not raw_expiry.strip():
            logger.warning("  Policy %s has no Expiration Date", policy_num)
            return STATUS_NEEDS_REVIEW, f"policy {policy_num} missing expiration date"

        expiry = parse_date(raw_expiry)
        if expiry is None:
            logger.warning("  Policy %s has unparseable date: '%s'", policy_num, raw_expiry)
            return STATUS_NEEDS_REVIEW, f"policy {policy_num} has unparseable date '{raw_expiry}'"

        if expiry < today:
            logger.warning("  Policy %s expired on %s", policy_num, expiry)
            # Expired beats Expiring Soon but keep looping in case another needs review
            worst = STATUS_EXPIRED
            reason = f"policy {policy_num} expired {expiry}"
            # Don't return yet — a missing-date policy would be Needs Review (higher priority)
            continue

        warning_threshold = today + timedelta(days=EXPIRY_WARNING_DAYS)
        if expiry <= warning_threshold:
            logger.info("  Policy %s expires soon: %s", policy_num, expiry)
            if worst == STATUS_COMPLIANT:
                worst = STATUS_EXPIRING_SOON
                reason = f"policy {policy_num} expires {expiry}"

    return worst, reason


# ── Core logic ────────────────────────────────────────────────────────────────

def fetch_all_policies(policies_table) -> dict:
    """
    Fetch all Insurance Policy records and index them by Airtable record ID.
    Returns {record_id: fields_dict}.
    """
    records = policies_table.all()
    indexed = {r["id"]: r["fields"] for r in records}
    logger.info("Loaded %d policy records from Airtable", len(indexed))
    return indexed


def get_vendor_policy_fields(vendor_fields: dict, all_policies: dict) -> List[dict]:
    """
    Given a vendor's fields dict and the full policy index, return the list
    of fields dicts for policies linked to this vendor via 'Vendor Link'.

    Airtable back-links: if the Vendors table has a linked field back to
    Insurance Policies, we use that. Otherwise we fall back to scanning
    all policies for a matching Vendor Link.
    """
    # If Airtable surfaces a linked-record field on the Vendor record, use it
    linked_ids = vendor_fields.get("Insurance Policies") or []
    if linked_ids:
        fields_list = []
        for rid in linked_ids:
            if rid in all_policies:
                fields_list.append(all_policies[rid])
            else:
                logger.warning("  Linked policy ID %s not found in loaded policies", rid)
        return fields_list

    # Fallback: no back-link field on vendor — return empty (will trigger Needs Review)
    return []


def run():
    logger.info("=== Module 5: Compliance Checker starting ===")

    # ── Validate env ──────────────────────────────────────────────────────────
    api_key  = (config.AIRTABLE_API_KEY  or "").strip()
    raw_base = (config.AIRTABLE_BASE_ID  or "").strip()

    if not api_key or not raw_base:
        raise EnvironmentError(
            "AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env"
        )

    base_id = clean_base_id(raw_base)
    logger.info("Airtable base ID : %s", base_id)

    api            = Api(api_key)
    vendors_table  = api.table(base_id, TABLE_VENDORS)
    policies_table = api.table(base_id, TABLE_POLICIES)

    today = date.today()
    logger.info("Evaluation date  : %s", today)

    # ── Load all policies once (avoids N+1 API calls) ─────────────────────────
    logger.info("Loading all Insurance Policies...")
    all_policies = fetch_all_policies(policies_table)

    # ── Load all vendors ──────────────────────────────────────────────────────
    logger.info("Loading all Vendors...")
    vendors = vendors_table.all()
    logger.info("Found %d vendor(s) to evaluate", len(vendors))

    if not vendors:
        logger.info("No vendors found. Nothing to do.")
        return

    # ── Evaluate each vendor ──────────────────────────────────────────────────
    counts = {
        STATUS_COMPLIANT:     0,
        STATUS_EXPIRING_SOON: 0,
        STATUS_EXPIRED:       0,
        STATUS_NEEDS_REVIEW:  0,
    }

    for vendor in vendors:
        vendor_id     = vendor["id"]
        vendor_fields = vendor["fields"]
        vendor_name   = vendor_fields.get("Vendor Name") or vendor_id

        logger.info("Evaluating vendor: %s", vendor_name)

        policy_fields = get_vendor_policy_fields(vendor_fields, all_policies)
        logger.info("  Linked policies : %d", len(policy_fields))

        new_status, reason = evaluate_compliance(policy_fields, today)
        logger.info("  Result          : %s (%s)", new_status, reason)

        current_status = vendor_fields.get("Compliance Status") or ""
        if current_status == new_status:
            logger.info("  No change needed (already '%s')", current_status)
        else:
            vendors_table.update(vendor_id, {"Expiration Status": new_status})
            logger.info(
                "  Updated '%s' → Compliance Status = '%s'",
                vendor_name, new_status,
            )

        counts[new_status] += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=== Compliance check complete ===")
    logger.info(
        "Summary — Compliant: %d | Expiring Soon: %d | Expired: %d | Needs Review: %d",
        counts[STATUS_COMPLIANT],
        counts[STATUS_EXPIRING_SOON],
        counts[STATUS_EXPIRED],
        counts[STATUS_NEEDS_REVIEW],
    )


if __name__ == "__main__":
    try:
        run()
    except (EnvironmentError, ValueError, RuntimeError) as exc:
        logger.error("Compliance check failed: %s", exc)
        raise SystemExit(1) from exc
