"""
report_corrections.py — Print a plain-text summary of the Corrections table.

Usage:
    .venv/bin/python report_corrections.py

Output is written to stdout only.  No writes to Airtable, no file output.
"""

from pathlib import Path
from datetime import date
from collections import Counter

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config
from airtable_importer import clean_base_id

CORRECTIONS_TABLE_ID = "tblh6NTQikjzl8FFu"


def _count_by(records: list[dict], field_name: str) -> list[tuple[str, int]]:
    """Return (value, count) pairs sorted descending by count.

    Missing / None values are bucketed under '(blank)'.
    """
    counter: Counter = Counter()
    for rec in records:
        val = rec.get("fields", {}).get(field_name) or "(blank)"
        counter[str(val).strip() or "(blank)"] += 1
    return counter.most_common()


def run() -> None:
    api_key = (config.AIRTABLE_API_KEY or "").strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)

    api = Api(api_key)
    table = api.table(base_id, CORRECTIONS_TABLE_ID)

    records = table.all()

    print("=== Corrections Report ===")
    print(f"Run date: {date.today().isoformat()}")
    print()

    if not records:
        print("No corrections yet.")
        return

    print(f"Total corrections: {len(records)}")
    print()

    # By field
    print("By field:")
    for val, count in _count_by(records, "Field"):
        print(f"  {val}: {count}")
    print()

    # By model
    print("By model:")
    for val, count in _count_by(records, "Model"):
        print(f"  {val}: {count}")
    print()

    # By Learning Status
    print("By Learning Status:")
    for val, count in _count_by(records, "Learning Status"):
        print(f"  {val}: {count}")
    print()

    # By source file (top 10)
    print("By source file (top 10):")
    for val, count in _count_by(records, "Source Filename")[:10]:
        print(f"  {val}: {count}")
    print()


if __name__ == "__main__":
    run()
