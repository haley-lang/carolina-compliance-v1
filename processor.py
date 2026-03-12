"""
Module 4 — COI Processor
Reads the newest "Imported" record from Airtable "Incoming Extractions",
matches the vendor, creates Insurance Policy and Certificate records,
and updates the extraction status.
"""

import json
import logging
import re
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from pyairtable import Api

# Load .env using an absolute path before importing config
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config  # noqa: E402 — must come after load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Table names ───────────────────────────────────────────────────────────────
TABLE_INCOMING   = "Incoming Extractions"
TABLE_VENDORS    = "Vendors"
TABLE_POLICIES   = "tblpPcmm5ANE0bMNB"
TABLE_CERTS      = "Insurance Certificates"


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


def get_tables(api: Api, base_id: str) -> dict:
    """Return a dict of named pyairtable Table objects."""
    return {
        name: api.table(base_id, name)
        for name in (TABLE_INCOMING, TABLE_VENDORS, TABLE_POLICIES, TABLE_CERTS)
    }


def fetch_newest_imported(table) -> Optional[dict]:
    """Return the most-recent Incoming Extraction with status 'Imported', or None.

    Fetches all Imported records then sorts in Python, using:
      1. "Extraction Processed At" field if present
      2. Airtable's built-in createdTime as fallback
    This avoids pyairtable version-specific sort= format issues.
    """
    records = table.all(formula="{Processing Status} = 'Imported'")
    if not records:
        return None

    def sort_key(record):
        # Prefer the explicit timestamp field; fall back to Airtable createdTime
        ts = record["fields"].get("Extraction Processed At") or ""
        if not ts:
            ts = record.get("createdTime") or ""
        return ts

    return max(records, key=sort_key)


def find_vendor(vendors_table, named_insured: str) -> Optional[dict]:
    """Case-insensitive match of named_insured against Vendor Name field."""
    needle = named_insured.strip().lower()
    all_vendors = vendors_table.all()
    for vendor in all_vendors:
        vendor_name = vendor["fields"].get("Vendor Name", "").strip().lower()
        if vendor_name == needle:
            return vendor
    return None


def policy_already_exists(policies_table, policy_number: str) -> bool:
    """Return True if a policy with this number already exists."""
    # Escape single quotes in the policy number for formula safety
    safe_num = policy_number.replace("'", "\\'")
    formula = f"{{Policy Number}} = '{safe_num}'"
    existing = policies_table.first(formula=formula)
    return existing is not None


def set_processing_status(incoming_table, record_id: str, status: str) -> None:
    """Update the Processing Status field on an Incoming Extractions record."""
    incoming_table.update(record_id, {"Processing Status": status})
    logger.info("Incoming Extraction %s → Processing Status = '%s'", record_id, status)


# ── Policy type normalisation ─────────────────────────────────────────────────

_POLICY_TYPE_MAP = {
    "commercial general liability": "General Liability",
    "general liability":            "General Liability",
    "workers compensation":         "Workers Compensation",
    "workers comp":                 "Workers Compensation",
    "automobile liability":         "Auto Liability",
    "auto liability":               "Auto Liability",
    "commercial auto":              "Auto Liability",
    "umbrella liability":           "Umbrella",
    "umbrella":                     "Umbrella",
}


def normalize_policy_type(raw: str) -> str:
    """Map raw extracted policy type to an Airtable-approved select value."""
    return _POLICY_TYPE_MAP.get(raw.strip().lower(), "Other")


# ── Core steps ────────────────────────────────────────────────────────────────

def process_policies(
    policies_table,
    policies: list,
    vendor_record_id: str,
    source_filename: str,
) -> list:
    """
    Create an Insurance Policy record for each policy in the list.
    Skips duplicates by Policy Number.
    Returns list of created record IDs.
    """
    created_ids = []
    for idx, policy in enumerate(policies, start=1):
        policy_number = (policy.get("policy_number") or "").strip()
        raw_type      = (policy.get("policy_type")   or "").strip()
        policy_type   = normalize_policy_type(raw_type)
        logger.info("Policy %d type: '%s' → '%s'", idx, raw_type, policy_type)

        if not policy_number:
            logger.warning("Policy %d has no policy number — skipping.", idx)
            continue

        if policy_already_exists(policies_table, policy_number):
            logger.info(
                "Policy %d (%s) already exists — skipping duplicate.", idx, policy_number
            )
            continue

        fields = {
            "Policy Record":           f"{policy_type} — {policy_number}",
            "Vendor Link":             [vendor_record_id],
            "Policy Type":             policy_type,
            "Policy Number":           policy_number,
            "Carrier":                 (policy.get("carrier")          or "").strip(),
            "Effective Date":          (policy.get("effective_date")   or "").strip(),
            "Expiration Date":         (policy.get("expiration_date")  or "").strip(),
            "Coverage Limits":         (policy.get("coverage_limits")  or "").strip(),
            "Policy Status Input":      "Active",
            "Certificate Source Filename": source_filename,
        }

        record = policies_table.create(fields)
        logger.info(
            "Created Insurance Policy — ID: %s  Policy #: %s",
            record["id"], policy_number,
        )
        created_ids.append(record["id"])

    return created_ids


def create_certificate(
    certs_table,
    vendor_record_id: str,
    named_insured: str,
    source_filename: str,
    certificate_date: str,
    policy_record_ids: list,
) -> dict:
    """Create one Insurance Certificate record linked to the Vendor."""
    fields = {
        "Vendor Link":      [vendor_record_id],
        "Named Insured":    named_insured,
        "Source Filename":  source_filename,
    }
    if certificate_date and certificate_date.strip():
        fields["Certificate Date"] = certificate_date.strip()
    # Link to policies if the table has a linked-record field for them
    if policy_record_ids:
        fields["Insurance Policies"] = policy_record_ids

    record = certs_table.create(fields)
    logger.info("Created Insurance Certificate — ID: %s", record["id"])
    return record


# ── Entry point ───────────────────────────────────────────────────────────────

def run():
    logger.info("=== Module 4: COI Processor starting ===")

    # ── Validate env ──────────────────────────────────────────────────────────
    api_key = (config.AIRTABLE_API_KEY or "").strip()
    raw_base = (config.AIRTABLE_BASE_ID or "").strip()

    if not api_key or not raw_base:
        raise EnvironmentError(
            "AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env"
        )

    base_id = clean_base_id(raw_base)
    logger.info("Airtable base ID : %s", base_id)

    api    = Api(api_key)
    tables = get_tables(api, base_id)

    # ── Step 1: fetch newest Imported extraction ───────────────────────────────
    logger.info("Step 1 — Fetching newest 'Imported' record from '%s'...", TABLE_INCOMING)
    extraction = fetch_newest_imported(tables[TABLE_INCOMING])

    if not extraction:
        logger.info("No records with Processing Status = 'Imported' found. Nothing to do.")
        return

    extraction_id     = extraction["id"]
    extraction_fields = extraction["fields"]
    named_insured     = (extraction_fields.get("Named Insured") or "").strip()
    source_filename   = (extraction_fields.get("Source Filename") or "").strip()
    raw_json_str      = extraction_fields.get("Raw JSON") or ""

    logger.info("Found extraction ID : %s", extraction_id)
    logger.info("Named Insured       : %s", named_insured)
    logger.info("Source Filename     : %s", source_filename)

    # ── Step 2: parse Raw JSON ────────────────────────────────────────────────
    logger.info("Step 2 — Parsing Raw JSON...")
    try:
        raw_data = json.loads(raw_json_str) if raw_json_str else {}
    except json.JSONDecodeError as exc:
        logger.error("Raw JSON is not valid JSON: %s", exc)
        set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
        return

    policies         = raw_data.get("policies") or []
    certificate_date = raw_data.get("certificate_date") or ""
    logger.info("Policies in JSON : %d", len(policies))

    # ── Step 3: match Vendor ──────────────────────────────────────────────────
    logger.info("Step 3 — Looking up Vendor for Named Insured: '%s'...", named_insured)
    vendor = find_vendor(tables[TABLE_VENDORS], named_insured)

    if not vendor:
        logger.warning(
            "No Vendor found matching '%s'. Setting status to 'Needs Review'.",
            named_insured,
        )
        set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
        logger.info("=== Module 4 complete (no vendor match) ===")
        return

    vendor_id   = vendor["id"]
    vendor_name = vendor["fields"].get("Vendor Name", "")
    logger.info("Vendor matched — ID: %s  Name: '%s'", vendor_id, vendor_name)

    # ── Step 4: create Insurance Policy records ────────────────────────────────
    logger.info("Step 4 — Creating Insurance Policy records...")
    if not policies:
        logger.warning("No policies found in Raw JSON — no policy records will be created.")

    created_policy_ids = process_policies(
        tables[TABLE_POLICIES],
        policies,
        vendor_id,
        source_filename,
    )
    logger.info(
        "Policies created: %d  (skipped as duplicates: %d)",
        len(created_policy_ids),
        len(policies) - len(created_policy_ids),
    )

    # ── Step 5: create Insurance Certificate record ───────────────────────────
    logger.info("Step 5 — Creating Insurance Certificate record...")
    create_certificate(
        tables[TABLE_CERTS],
        vendor_id,
        named_insured,
        source_filename,
        certificate_date,
        created_policy_ids,
    )

    # ── Step 6: mark extraction as Processed ──────────────────────────────────
    logger.info("Step 6 — Marking extraction as 'Processed'...")
    set_processing_status(tables[TABLE_INCOMING], extraction_id, "Processed")

    logger.info("=== Module 4 complete ===")


if __name__ == "__main__":
    try:
        run()
    except (EnvironmentError, ValueError, RuntimeError) as exc:
        logger.error("Processor failed: %s", exc)
        raise SystemExit(1) from exc
