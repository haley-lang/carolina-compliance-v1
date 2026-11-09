"""One-time backfill: populate Received At on Incoming Extractions from Incoming Documents.

For each Incoming Extractions record that is missing Received At, looks up the
matching Incoming Documents record via File Names (e.g. "uploads/abc.pdf" →
stem "abc" → matches Source Filename "abc.json"), and writes the Document's
Date Received into the extraction's Received At field.

Safe by design:
- Skips records that already have Received At populated.
- Never overwrites existing values.
- Dry-run mode (--dry-run) prints what would change without writing.
- Logs progress and a final summary.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TABLE_EXTRACTIONS = "tblT88Ty6d6M766oY"
TABLE_DOCUMENTS   = "Incoming Documents"
FIELD_RECEIVED_AT = "fldvEFuoyW7U9VWWO"  # Received At on Incoming Extractions


def build_doc_map(api: Api, base_id: str) -> dict[str, str]:
    """Return {stem + '.json': date_received_iso} from Incoming Documents."""
    doc_tbl = api.table(base_id, TABLE_DOCUMENTS)
    result: dict[str, str] = {}
    docs = doc_tbl.all(fields=["Date Received", "File Names"])
    for rec in docs:
        f = rec["fields"]
        date_received = (f.get("Date Received") or "").strip()
        if not date_received:
            continue
        for raw_path in (f.get("File Names") or "").split(","):
            stem = Path(raw_path.strip()).stem
            if stem:
                result[stem + ".json"] = date_received
    logger.info("Built doc map with %d entries", len(result))
    return result


def run(dry_run: bool = False) -> None:
    api_key = os.environ.get("AIRTABLE_API_KEY", "")
    base_id = os.environ.get("AIRTABLE_BASE_ID", "")
    if not api_key or not base_id:
        raise EnvironmentError("AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set")

    api = Api(api_key)
    ext_tbl = api.table(base_id, TABLE_EXTRACTIONS)

    doc_map = build_doc_map(api, base_id)

    logger.info("Fetching Incoming Extractions…")
    extractions = ext_tbl.all(fields=["Source Filename", "Received At"])
    logger.info("Found %d extraction records", len(extractions))

    updated = skipped_has_value = skipped_no_match = 0

    for rec in extractions:
        f = rec["fields"]
        if f.get("Received At"):
            skipped_has_value += 1
            continue

        source_filename = (f.get("Source Filename") or "").strip()
        if not source_filename:
            skipped_no_match += 1
            continue

        date_received = doc_map.get(source_filename)
        if not date_received:
            logger.debug("No doc match for %s", source_filename)
            skipped_no_match += 1
            continue

        if dry_run:
            logger.info("[DRY RUN] Would set Received At=%s on %s (%s)",
                        date_received, rec["id"], source_filename)
        else:
            try:
                ext_tbl.update(rec["id"], {FIELD_RECEIVED_AT: date_received})
                logger.info("Updated %s (%s) → %s", rec["id"], source_filename, date_received)
            except Exception as exc:
                logger.warning("Failed to update %s: %s", rec["id"], exc)
                continue
        updated += 1

    logger.info(
        "Done. updated=%d  skipped_already_set=%d  skipped_no_match=%d",
        updated, skipped_has_value, skipped_no_match,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Received At on Incoming Extractions")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
