"""Manually approve a Pending Review Incoming Extractions record so the
pipeline resumes downstream processing. Pre-dashboard interim mechanism.

What it does:
    - Sets Processing Status: "Pending Review" -> "Imported"  (resumes the gate)
    - Sets Review Status:     -> "Approved"
    - Sets Reviewed By:       "Haley"
    - Sets Reviewed At:       now() in ISO 8601 UTC
    - Optionally accepts --notes "..." to write Review Notes

Does NOT edit any extracted field values (no "Edit + Approve" capability —
that's dashboard work). For Edit-style approvals before the dashboard ships,
edit fields directly in Airtable, then run this script.

Idempotency guard: refuses to flip if Review Status != "Pending Review"
(prevents accidentally re-approving an already-Approved record).

Usage:
    .venv/bin/python manual_approve_extraction.py <record_id>
    .venv/bin/python manual_approve_extraction.py <record_id> --notes "edited carrier name"
    .venv/bin/python manual_approve_extraction.py <record_id> --dry-run
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

from pyairtable import Api  # noqa: E402

import config  # noqa: E402


def main(record_id: str, notes: str = "", dry_run: bool = False) -> int:
    api = Api(config.AIRTABLE_API_KEY)
    table = api.table(config.AIRTABLE_BASE_ID, "Incoming Extractions")

    try:
        record = table.get(record_id)
    except Exception as exc:
        print(f"[error] Could not fetch record {record_id}: {exc}", file=sys.stderr)
        return 2

    fields = record["fields"]
    current_review_status = (fields.get("Review Status") or "").strip()
    current_review_reason = (fields.get("Review Reason") or "").strip()
    current_processing_status = (fields.get("Processing Status") or "").strip()

    print(f"Record   : {record_id}")
    print(f"  Source Filename       : {fields.get('Source Filename', '(blank)')}")
    print(f"  Processing Status     : {current_processing_status!r}")
    print(f"  Review Status         : {current_review_status!r}")
    print(f"  Review Reason         : {current_review_reason!r}")
    print()

    # Idempotency guard
    if current_review_status != "Pending Review":
        print(
            f"[error] Review Status is {current_review_status!r}, not 'Pending Review'. "
            f"Refusing to flip — investigate before re-approving.",
            file=sys.stderr,
        )
        return 3

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    update_fields = {
        "Processing Status": "Imported",
        "Review Status": "Approved",
        "Reviewed By": "Haley",
        "Reviewed At": now_iso,
    }
    if notes:
        update_fields["Review Notes"] = notes

    print("Proposed update:")
    for k, v in update_fields.items():
        print(f"  {k:<22} -> {v!r}")
    print()

    if dry_run:
        print("[dry-run] No writes performed. Re-run without --dry-run to apply.")
        return 0

    try:
        table.update(record_id, update_fields, typecast=True)
    except Exception as exc:
        print(f"[error] Update failed: {exc}", file=sys.stderr)
        return 4

    print(f"[ok] Record {record_id} approved. Processor will pick it up next cycle.")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("record_id", help="Airtable Incoming Extractions record ID (recXXX...)")
    parser.add_argument("--notes", default="", help="Optional Review Notes to record")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would change without writing",
    )
    args = parser.parse_args()
    sys.exit(main(args.record_id, notes=args.notes, dry_run=args.dry_run))
