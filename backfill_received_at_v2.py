"""backfill_received_at_v2.py — Populate Received At on Incoming Extractions.

Extends the original backfill_received_at.py to handle the ~75 records that
have no matching Incoming Document. For those records the Airtable record's
own createdTime is used as the Received At value.

For every record updated from createdTime, an "Upload Label" of
"Uploaded <date>" is printed in the final report (no new Airtable field is
created — this is a printed annotation only).

Three outcome buckets:
  from document     — matched via File Names in Incoming Documents
  from creation date — no matching document; fell back to record createdTime
  already set       — Received At was already populated; skipped

Usage:
    python backfill_received_at_v2.py           # live run
    python backfill_received_at_v2.py --dry-run # print changes, no writes
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

# ── Table / field identifiers ─────────────────────────────────────────────────

TABLE_EXTRACTIONS = "tblT88Ty6d6M766oY"
TABLE_DOCUMENTS   = "Incoming Documents"
FIELD_RECEIVED_AT = "fldvEFuoyW7U9VWWO"  # Received At on Incoming Extractions


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _upload_label(iso_ts: str) -> str:
    """Format 'Uploaded YYYY-MM-DD' from an ISO-8601 timestamp string."""
    # createdTime from Airtable is e.g. "2024-03-15T12:00:00.000Z"
    date_part = iso_ts[:10] if iso_ts else "unknown"
    return f"Uploaded {date_part}"


# ── Main logic ────────────────────────────────────────────────────────────────

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

    # Outcome buckets.
    from_document:     list[tuple[str, str, str]] = []  # (rec_id, source_filename, date)
    from_creation_date: list[tuple[str, str, str]] = []  # (rec_id, label, iso_ts)
    already_set:        list[str] = []                   # rec_ids

    for rec in extractions:
        f = rec["fields"]

        # Bucket 1: already has a value — skip.
        if f.get("Received At"):
            already_set.append(rec["id"])
            continue

        source_filename = (f.get("Source Filename") or "").strip()

        # Bucket 2: matches the doc map — use Date Received.
        if source_filename and source_filename in doc_map:
            date_received = doc_map[source_filename]
            from_document.append((rec["id"], source_filename, date_received))
            continue

        # Bucket 3: no match — fall back to createdTime.
        created_time = rec.get("createdTime", "")
        label = _upload_label(created_time)
        from_creation_date.append((rec["id"], label, created_time))

    # ── Report dry-run results ────────────────────────────────────────────────
    if dry_run:
        print("\n[DRY RUN] No records will be updated.\n")
        print(f"Already set (would skip) : {len(already_set)}")
        print(f"From document            : {len(from_document)}")
        for rec_id, fname, date in from_document:
            print(f"  {rec_id}  {fname}  →  {date}")
        print(f"From creation date       : {len(from_creation_date)}")
        for rec_id, label, ts in from_creation_date:
            print(f"  {rec_id}  {label}  (createdTime={ts})")
        return

    # ── Live run — write updates ──────────────────────────────────────────────
    updated_from_doc  = 0
    updated_from_ctime = 0

    for rec_id, source_filename, date_received in from_document:
        try:
            ext_tbl.update(rec_id, {FIELD_RECEIVED_AT: date_received})
            logger.info("Updated %s (%s) → %s [from document]",
                        rec_id, source_filename, date_received)
            updated_from_doc += 1
        except Exception as exc:
            logger.warning("Failed to update %s: %s", rec_id, exc)

    for rec_id, label, created_time in from_creation_date:
        try:
            ext_tbl.update(rec_id, {FIELD_RECEIVED_AT: created_time})
            logger.info("Updated %s → %s [from creation date, label: %s]",
                        rec_id, created_time, label)
            updated_from_ctime += 1
        except Exception as exc:
            logger.warning("Failed to update %s: %s", rec_id, exc)

    # ── Final report ──────────────────────────────────────────────────────────
    print("\n── Backfill Received At v2 — Summary ──────────────────────")
    print(f"  already set        : {len(already_set)}")
    print(f"  from document      : {updated_from_doc} updated  "
          f"({len(from_document)} matched)")
    print(f"  from creation date : {updated_from_ctime} updated  "
          f"({len(from_creation_date)} matched)")
    print()
    if from_creation_date:
        print("Upload Labels (from creation date bucket):")
        for rec_id, label, ts in from_creation_date:
            status = "updated" if updated_from_ctime > 0 else "attempted"
            print(f"  {rec_id}  {label}")
    print("────────────────────────────────────────────────────────────")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill Received At on Incoming Extractions (v2, with createdTime fallback)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing any records.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
