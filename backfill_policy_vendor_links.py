"""One-time backfill script to link Insurance Policies to Vendors.

Behavior (safe by design):
- Reads all Insurance Policies.
- Only processes policies that do NOT already have a Vendor link.
- Tries deterministic matching only (no fuzzy logic):
  1) Prefer vendor IDs linked through related certificate fields on the policy.
  2) Fallback to exact vendor-name matching using policy name fields.
- Updates policy Vendor field only when exactly one deterministic match exists.
- Skips and logs when no match or ambiguous match.
- Never creates vendors.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from dotenv import load_dotenv
from pyairtable import Api


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


TABLE_POLICIES = "Insurance Policies"
TABLE_VENDORS = "Vendors"
TABLE_CERTIFICATES = "Insurance Certificates"

# Policy fields that may contain linked certificate IDs.
POLICY_CERT_LINK_FIELDS = [
    "Insurance Certificate",
    "Insurance Certificates",
    "Certificate",
    "Certificates",
]

# Policy fields that may contain a vendor-identifying name.
POLICY_VENDOR_NAME_FIELDS = [
    "Vendor Name",
    "Named Insured",
]


def clean_base_id(raw: str) -> str:
    """Extract appXXXXXXXXXXXX from AIRTABLE_BASE_ID (supports pasted Airtable URLs)."""
    match = re.search(r"(app[A-Za-z0-9]{10,})", raw or "")
    if not match:
        raise ValueError(
            f"AIRTABLE_BASE_ID does not look valid: '{raw}'. Expected value like appXXXXXXXXXXXX."
        )
    return match.group(1)


def require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def linked_record_ids(value: Any) -> List[str]:
    """Return Airtable linked-record IDs from list[str] or list[{id: ...}]."""
    if not isinstance(value, list):
        return []

    ids: List[str] = []
    for item in value:
        if isinstance(item, str) and item:
            ids.append(item)
        elif isinstance(item, dict) and isinstance(item.get("id"), str):
            ids.append(item["id"])
    return ids


def table_field_names(table) -> Set[str]:
    try:
        schema = table.schema()
        return {field.name for field in schema.fields}
    except Exception as exc:
        logger.warning("Could not load schema for '%s': %s", table.name, exc)
        return set()


def detect_policy_vendor_field(policy_field_names: Set[str]) -> str:
    """Prefer 'Vendor' field, fallback to 'Vendor Link'."""
    if "Vendor" in policy_field_names:
        return "Vendor"
    if "Vendor Link" in policy_field_names:
        return "Vendor Link"
    raise RuntimeError("Policy table is missing both 'Vendor' and 'Vendor Link' fields.")


def existing_policy_vendor_links(fields: Dict[str, Any]) -> List[str]:
    return linked_record_ids(fields.get("Vendor")) + linked_record_ids(fields.get("Vendor Link"))


def build_vendor_name_index(vendors: Iterable[Dict[str, Any]]) -> Dict[str, Set[str]]:
    index: Dict[str, Set[str]] = {}
    for vendor in vendors:
        vendor_id = vendor.get("id")
        if not vendor_id:
            continue

        name = normalize_text((vendor.get("fields") or {}).get("Vendor Name", ""))
        if not name:
            continue

        index.setdefault(name, set()).add(vendor_id)
    return index


def build_certificate_vendor_index(
    certificates: Iterable[Dict[str, Any]],
    cert_vendor_fields: List[str],
) -> Dict[str, Set[str]]:
    index: Dict[str, Set[str]] = {}
    for cert in certificates:
        cert_id = cert.get("id")
        if not cert_id:
            continue

        fields = cert.get("fields") or {}
        vendor_ids: Set[str] = set()
        for field in cert_vendor_fields:
            vendor_ids.update(linked_record_ids(fields.get(field)))

        if vendor_ids:
            index[cert_id] = vendor_ids
    return index


def run() -> None:
    load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

    api_key = require_env("AIRTABLE_API_KEY")
    base_id = clean_base_id(require_env("AIRTABLE_BASE_ID"))

    api = Api(api_key)
    policies_table = api.table(base_id, TABLE_POLICIES)
    vendors_table = api.table(base_id, TABLE_VENDORS)
    certificates_table = api.table(base_id, TABLE_CERTIFICATES)

    policy_field_names = table_field_names(policies_table)
    certificate_field_names = table_field_names(certificates_table)
    vendor_update_field = detect_policy_vendor_field(policy_field_names)

    cert_policy_link_fields = [f for f in POLICY_CERT_LINK_FIELDS if f in policy_field_names]
    cert_vendor_fields = [f for f in ("Vendor", "Vendor Link") if f in certificate_field_names]
    policy_vendor_name_fields = [f for f in POLICY_VENDOR_NAME_FIELDS if f in policy_field_names]

    logger.info("Using policy vendor update field: %s", vendor_update_field)
    logger.info("Policy certificate link fields in use: %s", cert_policy_link_fields or "(none)")
    logger.info("Certificate vendor link fields in use: %s", cert_vendor_fields or "(none)")
    logger.info("Policy vendor name fields in use: %s", policy_vendor_name_fields or "(none)")

    policies = policies_table.all()
    vendors = vendors_table.all()
    certificates = certificates_table.all()

    vendor_name_index = build_vendor_name_index(vendors)
    cert_vendor_index = build_certificate_vendor_index(certificates, cert_vendor_fields)

    policies_scanned = 0
    vendor_links_repaired = 0
    skipped_no_match = 0
    skipped_ambiguous = 0

    for policy in policies:
        policies_scanned += 1
        policy_id = policy.get("id")
        fields = policy.get("fields") or {}
        policy_label = fields.get("Policy Number") or policy_id

        # Safety: do not modify policies that already have a vendor link.
        if existing_policy_vendor_links(fields):
            continue

        # 1) Preferred deterministic source: certificate/vendor links.
        cert_candidates: Set[str] = set()
        for cert_link_field in cert_policy_link_fields:
            for cert_id in linked_record_ids(fields.get(cert_link_field)):
                cert_candidates.update(cert_vendor_index.get(cert_id, set()))

        if len(cert_candidates) == 1:
            vendor_id = next(iter(cert_candidates))
            policies_table.update(policy_id, {vendor_update_field: [vendor_id]})
            vendor_links_repaired += 1
            logger.info(
                "Repaired policy %s using certificate-linked vendor: %s",
                policy_label,
                vendor_id,
            )
            continue

        if len(cert_candidates) > 1:
            skipped_ambiguous += 1
            logger.warning(
                "Skipped policy %s: ambiguous certificate vendor candidates %s",
                policy_label,
                sorted(cert_candidates),
            )
            continue

        # 2) Fallback deterministic source: exact vendor name field match.
        name_candidates: Set[str] = set()
        for name_field in policy_vendor_name_fields:
            raw_name = fields.get(name_field)
            if not isinstance(raw_name, str):
                continue
            normalized = normalize_text(raw_name)
            if normalized:
                name_candidates.update(vendor_name_index.get(normalized, set()))

        if len(name_candidates) == 1:
            vendor_id = next(iter(name_candidates))
            policies_table.update(policy_id, {vendor_update_field: [vendor_id]})
            vendor_links_repaired += 1
            logger.info(
                "Repaired policy %s using exact vendor name match: %s",
                policy_label,
                vendor_id,
            )
            continue

        if len(name_candidates) == 0:
            skipped_no_match += 1
            logger.warning("Skipped policy %s: no deterministic vendor match", policy_label)
        else:
            skipped_ambiguous += 1
            logger.warning(
                "Skipped policy %s: ambiguous vendor name matches %s",
                policy_label,
                sorted(name_candidates),
            )

    logger.info("Policies scanned: %s", policies_scanned)
    logger.info("Vendor links repaired: %s", vendor_links_repaired)
    logger.info("Skipped (no match): %s", skipped_no_match)
    logger.info("Skipped (ambiguous): %s", skipped_ambiguous)


if __name__ == "__main__":
    run()
