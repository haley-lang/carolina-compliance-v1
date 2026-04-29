"""Compliance checker based only on expiration dates.

Status priority:
1) expiration_date missing -> "missing"
2) expiration_date < today -> "expired"
3) expiration_date within 30 days -> "expiring soon"
4) else -> "compliant"
"""

import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TABLE_VENDORS = "Vendors"
TABLE_POLICIES = "Insurance Policies"
EXPIRY_WARNING_DAYS = 30

STATUS_MISSING = "Missing Coverage"
STATUS_EXPIRED = "Expired"
STATUS_EXPIRING_SOON = "Expiring Soon"
STATUS_COMPLIANT = "Matches Requirements"

_DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"]


def clean_base_id(raw: str) -> str:
    match = re.search(r"(app[A-Za-z0-9]{10,})", raw)
    if not match:
        raise ValueError(
            f"AIRTABLE_BASE_ID does not look like a valid base ID: '{raw}'. "
            "It should start with 'app'."
        )
    return match.group(1)


def parse_date(raw: str) -> Optional[date]:
    cleaned = raw.strip() if raw else ""
    if not cleaned:
        return None

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def status_from_expiration(expiration_date_raw: str, today: date) -> str:
    """Map a raw expiration date string to a deterministic compliance status."""
    raw = (expiration_date_raw or "").strip()
    expiry = parse_date(raw)

    if not raw or expiry is None:
        return STATUS_MISSING

    if expiry < today:
        return STATUS_EXPIRED

    if expiry <= (today + timedelta(days=EXPIRY_WARNING_DAYS)):
        return STATUS_EXPIRING_SOON

    return STATUS_COMPLIANT


def evaluate_compliance(policies: List[dict], today: date) -> str:
    """Return deterministic status based ONLY on expiration_date rules."""
    if not policies:
        return STATUS_MISSING

    status_priority = {
        STATUS_COMPLIANT: 0,
        STATUS_EXPIRING_SOON: 1,
        STATUS_EXPIRED: 2,
        STATUS_MISSING: 3,
    }
    resolved_status = STATUS_COMPLIANT

    for policy in policies:
        raw_expiry = policy.get("expiration_date") or policy.get("Expiration Date") or ""
        policy_status = status_from_expiration(raw_expiry, today)

        if status_priority[policy_status] > status_priority[resolved_status]:
            resolved_status = policy_status
            if resolved_status == STATUS_MISSING:
                break

    return resolved_status


def fetch_all_policies(policies_table) -> dict:
    records = policies_table.all()
    return {record["id"]: record["fields"] for record in records}


def get_vendor_policy_fields(vendor_fields: dict, all_policies: dict) -> List[dict]:
    linked_ids = vendor_fields.get("Insurance Policies") or []
    if not linked_ids:
        return []
    return [all_policies[rid] for rid in linked_ids if rid in all_policies]


def run() -> None:
    api_key = (config.AIRTABLE_API_KEY or "").strip()
    raw_base = (config.AIRTABLE_BASE_ID or "").strip()
    if not api_key or not raw_base:
        raise EnvironmentError("AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env")

    base_id = clean_base_id(raw_base)
    api = Api(api_key)
    vendors_table = api.table(base_id, TABLE_VENDORS)
    policies_table = api.table(base_id, TABLE_POLICIES)

    today = date.today()
    all_policies = fetch_all_policies(policies_table)
    vendors = vendors_table.all()

    for vendor in vendors:
        vendor_id = vendor["id"]
        vendor_fields = vendor.get("fields", {})
        policy_fields = get_vendor_policy_fields(vendor_fields, all_policies)

        new_status = evaluate_compliance(policy_fields, today)
        current_status = vendor_fields.get("Compliance Status") or ""

        if current_status != new_status:
            vendors_table.update(vendor_id, {"Compliance Status": new_status}, typecast=True)
            logger.info("Updated %s -> %s", vendor_id, new_status)


if __name__ == "__main__":
    try:
        run()
    except (EnvironmentError, ValueError, RuntimeError) as exc:
        logger.error("Compliance check failed: %s", exc)
        raise SystemExit(1) from exc
