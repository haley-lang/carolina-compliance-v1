"""cleanup_compliance_log.py — Back up and deduplicate the Compliance Log table.

Base:  appCGgww0Pt7KE04u
Table: tblxdt7DT6V3JCQcW (Compliance Log)

Dedup rule: within each (vendor_link_id, client_link_id) group, keep the
first record and any later record where Decision OR Failure Reasons changed
from the previous kept record. Delete the rest.

Usage:
    python cleanup_compliance_log.py           # live run
    python cleanup_compliance_log.py --dry-run # count-only, no deletes
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

# ── Airtable identifiers ──────────────────────────────────────────────────────

BASE_ID  = "appCGgww0Pt7KE04u"
TABLE_ID = "tblxdt7DT6V3JCQcW"

FIELD_LOG_ENTRY       = "fldzUTll7izbXfBEB"  # Log Entry (primary)
FIELD_TIMESTAMP       = "fldWzdPiDQOXqlwhg"  # Timestamp (dateTime)
FIELD_VENDOR_NAME     = "fldu8DcdwPbDxCvpb"  # Vendor Name
FIELD_CLIENT_NAME     = "fldcbzhSPWdOBanym"  # Client Name
FIELD_DECISION        = "fld6Ml1MK3QpmIICK"  # Decision (singleSelect)
FIELD_FAILURE_REASONS = "fldoTDHEEkekT2jAj"  # Failure Reasons (multilineText)
FIELD_PREV_DECISION   = "fld4HwkZfiYTwVnW6"  # Previous Decision
FIELD_DECISION_CHANGED= "fld7lWbXuLhKg46Dk"  # Decision Changed (checkbox)
FIELD_VENDOR_LINK     = "fldkq8vZZsN6uBTL0"  # Vendor Link (linked records)
FIELD_CLIENT_LINK     = "fldbKVYBi20p6wykZ"  # Client Link (linked records)

ALL_FIELDS = [
    FIELD_LOG_ENTRY, FIELD_TIMESTAMP, FIELD_VENDOR_NAME, FIELD_CLIENT_NAME,
    FIELD_DECISION, FIELD_FAILURE_REASONS, FIELD_PREV_DECISION,
    FIELD_DECISION_CHANGED, FIELD_VENDOR_LINK, FIELD_CLIENT_LINK,
]

BATCH_SIZE   = 10
BATCH_SLEEP  = 0.1  # seconds between delete batches


# ── Helpers ───────────────────────────────────────────────────────────────────

def _first_linked_id(field_value) -> str:
    """Return the first record ID from a linked-records field, or ''."""
    if isinstance(field_value, list) and field_value:
        return field_value[0]
    return ""


def _parse_ts(value: str | None) -> datetime:
    """Parse an ISO-8601 dateTime string from Airtable into a timezone-aware datetime."""
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    # Airtable returns e.g. "2024-03-15T12:00:00.000Z"
    value = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _decision_name(field_value) -> str:
    """Extract the name string from a singleSelect field value."""
    if isinstance(field_value, dict):
        return field_value.get("name", "") or ""
    return str(field_value) if field_value else ""


def _write_backup(records: list[dict], path: Path) -> None:
    """Write all records to a CSV file for safe-keeping before any deletes."""
    if not records:
        return
    # Determine which field IDs are actually present in the fetched records.
    fetched_fields = sorted({k for r in records for k in r.get("fields", {})})
    fieldnames = ["airtable_id", "created_time"] + fetched_fields
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            row = {"airtable_id": rec["id"], "created_time": rec.get("createdTime", "")}
            f = rec.get("fields", {})
            for fid in fetched_fields:
                val = f.get(fid, "")
                if isinstance(val, list):
                    val = "|".join(str(v) for v in val)
                elif isinstance(val, dict):
                    val = val.get("name", str(val))
                row[fid] = val
            writer.writerow(row)


# ── Core logic ────────────────────────────────────────────────────────────────

def build_delete_set(records: list[dict]) -> set[str]:
    """Return the set of record IDs to delete based on dedup rules."""
    # Group by (vendor_id, client_id).
    groups: dict[tuple[str, str], list[dict]] = {}
    for rec in records:
        f = rec.get("fields", {})
        vendor_id = _first_linked_id(f.get(FIELD_VENDOR_LINK))
        client_id = _first_linked_id(f.get(FIELD_CLIENT_LINK))
        key = (vendor_id, client_id)
        groups.setdefault(key, []).append(rec)

    to_delete: set[str] = set()

    for (vendor_id, client_id), group in groups.items():
        # Sort by Timestamp ascending (oldest first).
        group.sort(key=lambda r: _parse_ts(r.get("fields", {}).get(FIELD_TIMESTAMP)))

        last_decision: str | None = None
        last_failure: str | None = None
        kept_first = False

        for rec in group:
            f = rec.get("fields", {})
            decision = _decision_name(f.get(FIELD_DECISION))
            failure  = (f.get(FIELD_FAILURE_REASONS) or "").strip()

            if not kept_first:
                # Always keep the chronological first record.
                last_decision = decision
                last_failure  = failure
                kept_first = True
                continue

            # Keep if Decision OR Failure Reasons changed from previous kept record.
            if decision != last_decision or failure != last_failure:
                last_decision = decision
                last_failure  = failure
            else:
                to_delete.add(rec["id"])

    return to_delete


def run(dry_run: bool = False) -> None:
    api_key = os.environ.get("AIRTABLE_API_KEY", "")
    if not api_key:
        raise EnvironmentError("AIRTABLE_API_KEY must be set in .env")

    start = time.time()
    api   = Api(api_key)
    tbl   = api.table(BASE_ID, TABLE_ID)

    # 1. Load all records (retry on 504 / transient errors).
    DEDUP_FIELDS = [
        FIELD_TIMESTAMP, FIELD_VENDOR_LINK, FIELD_CLIENT_LINK,
        FIELD_DECISION, FIELD_FAILURE_REASONS,
    ]
    print("Fetching all Compliance Log records…")
    records = None
    for attempt in range(1, 5):
        try:
            records = tbl.all(fields=DEDUP_FIELDS, use_field_ids=True)
            break
        except Exception as exc:
            print(f"  Fetch attempt {attempt} failed: {exc}")
            if attempt < 4:
                sleep_s = 15 * attempt
                print(f"  Retrying in {sleep_s}s…")
                time.sleep(sleep_s)
    if records is None:
        raise RuntimeError("Could not fetch Compliance Log after 4 attempts")
    total = len(records)
    print(f"Loaded {total} records.")

    # 2. Write CSV backup (minimal — only dedup fields).
    ts_str  = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(f"compliance_log_backup_{ts_str}.csv")
    _write_backup(records, backup_path)
    print(f"Backup written to: {backup_path.resolve()}")

    # 3 + 4. Determine which records to delete.
    to_delete = build_delete_set(records)
    keep_count   = total - len(to_delete)
    delete_count = len(to_delete)

    print(f"\nTotal records  : {total}")
    print(f"Records to keep: {keep_count}")
    print(f"Records to delete: {delete_count}")

    if dry_run:
        elapsed = time.time() - start
        print(f"\n[DRY RUN] No records deleted. Time taken: {elapsed:.1f}s")
        return

    # 5. Batch delete.
    delete_ids   = list(to_delete)
    deleted_count = 0

    for i in range(0, len(delete_ids), BATCH_SIZE):
        batch = delete_ids[i : i + BATCH_SIZE]
        try:
            tbl.batch_delete(batch)
            deleted_count += len(batch)
            print(f"Deleted batch {i // BATCH_SIZE + 1}: {len(batch)} records "
                  f"({deleted_count}/{delete_count} total)")
        except Exception as exc:
            print(f"ERROR deleting batch starting at index {i}: {exc}")
        time.sleep(BATCH_SLEEP)

    elapsed = time.time() - start
    print(f"\nDone.")
    print(f"  Total records  : {total}")
    print(f"  Records kept   : {keep_count}")
    print(f"  Records to delete: {delete_count}")
    print(f"  Records deleted: {deleted_count}")
    print(f"  Time taken     : {elapsed:.1f}s")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backup and deduplicate the Compliance Log table in Airtable."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts without deleting any records.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
