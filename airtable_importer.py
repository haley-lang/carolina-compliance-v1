"""
Module 3 — Airtable Importer
Reads the newest JSON file from extracted/ and creates a record
in the Airtable table "Incoming Extractions".
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load .env before importing config so all variables are present
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

EXTRACTED_DIR = Path("extracted")
INCOMING_EXTRACTIONS_TABLE = "Incoming Extractions"


def get_newest_json(directory: Path) -> Path:
    """Return the most recently modified .json file in directory."""
    json_files = list(directory.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in '{directory}/'")
    return max(json_files, key=lambda f: f.stat().st_mtime)


def load_json_safe(path: Path) -> dict:
    """Load and parse a JSON file, raising on invalid content."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Expected a JSON object, got {type(data).__name__}")
        return data
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in '{path}': {exc}") from exc


def build_fields(source_filename: str, data: dict, raw_json: str) -> dict:
    """Map extracted JSON fields to Airtable field names."""
    contact_emails = data.get("contact_emails") or []
    if isinstance(contact_emails, list):
        contact_emails_str = ", ".join(str(e) for e in contact_emails)
    else:
        contact_emails_str = str(contact_emails)

    policies = data.get("policies") or []
    policies_count = len(policies) if isinstance(policies, list) else 0

    return {
        "Source Filename": source_filename,
        "Document Type": data.get("document_type") or "",
        "Named Insured": data.get("named_insured") or "",
        "Contact Emails": contact_emails_str,
        "Policies Count": policies_count,
        "Raw JSON": raw_json,
        "Extraction Processed At": datetime.now(timezone.utc).isoformat(),
        "Processing Status": "Imported",
    }


def clean_base_id(raw: str) -> str:
    """Extract just the appXXXXXXXXXXXXXX portion from the base ID.

    Guards against users accidentally pasting a full Airtable UI URL
    (e.g. appABC123/tblXXX/viwYYY?blocks=hide) instead of the plain base ID.
    """
    match = re.search(r"(app[A-Za-z0-9]{10,})", raw)
    if not match:
        raise ValueError(
            f"AIRTABLE_BASE_ID does not look like a valid Airtable base ID: '{raw}'\n"
            "It should start with 'app' followed by alphanumeric characters "
            "(e.g. appCGgww0Pt7KE04u)."
        )
    return match.group(1)


def push_to_airtable(base_id: str, api_key: str, fields: dict) -> dict:
    """Create a record in the 'Incoming Extractions' table via the REST API."""
    url = f"https://api.airtable.com/v0/{base_id}/{INCOMING_EXTRACTIONS_TABLE}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Serialize manually so Content-Type and body are unambiguous
    body = json.dumps({"fields": fields})

    logger.info("POST %s", url)
    response = requests.post(url, headers=headers, data=body.encode("utf-8"), timeout=30)

    if not response.ok:
        raise RuntimeError(
            f"Airtable API error {response.status_code}: {response.text}"
        )

    return response.json()


def run():
    logger.info("=== Module 3: Airtable Importer starting ===")

    # Validate credentials
    if not config.AIRTABLE_API_KEY or not config.AIRTABLE_BASE_ID:
        raise EnvironmentError(
            "AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env"
        )

    token = (config.AIRTABLE_API_KEY or "").strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)

    # Debug: confirm auth config without exposing the full token
    logger.info("--- Airtable config debug ---")
    logger.info("Token present    : %s", bool(token))
    logger.info("Token prefix     : %s", token[:4] if token else "N/A")
    logger.info("Token length     : %d chars", len(token))
    logger.info("Auth header fmt  : Bearer %s...", token[:4] if token else "N/A")
    logger.info("Base ID          : %s", base_id)
    logger.info("Table name       : %s", INCOMING_EXTRACTIONS_TABLE)
    logger.info("-----------------------------")

    if not EXTRACTED_DIR.exists():
        raise FileNotFoundError(
            f"Directory '{EXTRACTED_DIR}/' does not exist. "
            "Run Module 2 (extractor.py) first."
        )

    # Step 1: find newest JSON
    json_path = get_newest_json(EXTRACTED_DIR)
    logger.info("Newest extraction file: %s", json_path.name)

    # Step 2: parse JSON
    data = load_json_safe(json_path)
    logger.info(
        "Parsed — document_type=%s, named_insured=%s, policies=%d",
        data.get("document_type"),
        data.get("named_insured"),
        len(data.get("policies") or []),
    )

    # Step 3: build raw JSON string and field map
    raw_json = json.dumps(data, indent=2)
    fields = build_fields(json_path.name, data, raw_json)

    # Step 3b: preflight — verify token can read the table before writing
    preflight_url = (
        f"https://api.airtable.com/v0/{base_id}/{INCOMING_EXTRACTIONS_TABLE}"
        "?maxRecords=1"
    )
    preflight_headers = {"Authorization": f"Bearer {token}"}
    logger.info("--- Preflight table check ---")
    logger.info("GET %s", preflight_url)
    preflight = requests.get(preflight_url, headers=preflight_headers, timeout=30)
    if preflight.ok:
        logger.info("Preflight PASSED — table is reachable (HTTP %s)", preflight.status_code)
    else:
        logger.error(
            "Preflight FAILED — HTTP %s: %s",
            preflight.status_code,
            preflight.text,
        )
        raise RuntimeError(
            f"Cannot reach table '{INCOMING_EXTRACTIONS_TABLE}' "
            f"(HTTP {preflight.status_code}). "
            "Check that the table name is exact, the token has read+write access "
            "to this base, and the token scope includes 'data.records:write'."
        )
    logger.info("-----------------------------")

    # Step 4: push to Airtable
    logger.info("Creating record in Airtable table '%s'...", INCOMING_EXTRACTIONS_TABLE)
    record = push_to_airtable(base_id, token, fields)
    logger.info("Record created — Airtable ID: %s", record["id"])
    logger.info("=== Module 3 complete ===")
    return record


if __name__ == "__main__":
    try:
        run()
    except (FileNotFoundError, ValueError, EnvironmentError, RuntimeError) as exc:
        logger.error("Import failed: %s", exc)
        raise SystemExit(1) from exc
