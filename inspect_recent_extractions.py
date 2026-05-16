"""Inspect the N most recent Incoming Extractions records.

Quick-look tool for verifying confidence values, processing status, and
recency during Phase 1B/1C/1E work without having to open Airtable in a
browser.

Usage:
    .venv/bin/python inspect_recent_extractions.py        # default 10 rows
    .venv/bin/python inspect_recent_extractions.py 25     # 25 rows

Output columns: Source Filename, Confidence Score, Processing Status,
Extraction Processed At (sorted newest-first).
"""

import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

from pyairtable import Api  # noqa: E402  (load_dotenv must run first)

import config  # noqa: E402


def main(n: int = 10) -> None:
    api = Api(config.AIRTABLE_API_KEY)
    table = api.table(config.AIRTABLE_BASE_ID, "Incoming Extractions")
    records = table.all(
        fields=[
            "Source Filename",
            "Confidence Score",
            "Processing Status",
            "Extraction Processed At",
        ],
        sort=["-Extraction Processed At"],
        max_records=n,
    )

    print(f"{'SOURCE FILENAME':<55} {'CONFIDENCE':>10} {'STATUS':<18} PROCESSED AT")
    print("-" * 110)
    for r in records:
        f = r["fields"]
        name = (f.get("Source Filename") or "")[:54]
        conf = f.get("Confidence Score")
        conf_str = f"{conf:.2f}" if isinstance(conf, (int, float)) else "—"
        status = (f.get("Processing Status") or "")[:17]
        processed = (f.get("Extraction Processed At") or "")[:19]
        print(f"{name:<55} {conf_str:>10} {status:<18} {processed}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    main(n)
