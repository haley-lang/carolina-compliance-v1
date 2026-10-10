"""
export_gold_candidates.py — Export "New" corrections as gold-set candidates.

Reads the Corrections table, filters for rows where Learning Status = "New",
and writes to tests/gold/rtr_gold_candidates.json.

NEVER overwrites tests/gold/rtr_gold.json — that file is Haley's approved gold set.

Usage:
    .venv/bin/python export_gold_candidates.py

No writes to Airtable, no Anthropic API calls, no email.
"""

import json
from pathlib import Path

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config
from airtable_importer import clean_base_id

CORRECTIONS_TABLE_ID = "tblh6NTQikjzl8FFu"

# Output path — NEVER rtr_gold.json
GOLD_DIR = Path(__file__).parent / "tests" / "gold"
CANDIDATES_PATH = GOLD_DIR / "rtr_gold_candidates.json"

# Safety guard: we must never write to this path
_FORBIDDEN_PATH = GOLD_DIR / "rtr_gold.json"


def _build_candidate(rec: dict) -> dict:
    fields = rec.get("fields", {})
    return {
        "source_filename": fields.get("Source Filename"),
        "record_id": fields.get("Incoming Extraction Record ID"),
        "field_path": fields.get("Field"),
        "old_value": fields.get("Old Value"),
        "corrected_value": fields.get("New Value"),
        "why": fields.get("Why (Haley's note)"),
        "model": fields.get("Model"),
        "corrected_at": fields.get("Corrected At"),
        "source_page_url": None,   # image URL requires separate lookup
        "status": "candidate",     # indicates this needs Haley's approval
    }


def run() -> None:
    # Safety invariant — should never be triggered, but belt-and-suspenders
    assert str(CANDIDATES_PATH.resolve()) != str(_FORBIDDEN_PATH.resolve()), (
        "BUG: attempted to write to rtr_gold.json — aborting"
    )

    api_key = (config.AIRTABLE_API_KEY or "").strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)

    api = Api(api_key)
    table = api.table(base_id, CORRECTIONS_TABLE_ID)

    records = table.all()

    new_records = [r for r in records if r.get("fields", {}).get("Learning Status") == "New"]

    # Ensure output directory exists
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    if not new_records:
        payload = {
            "note": "No 'New' corrections found — run after Haley has submitted corrections.",
            "candidates": [],
        }
        CANDIDATES_PATH.write_text(json.dumps(payload, indent=2))
        print(f"0 candidates written to {CANDIDATES_PATH}")
        return

    candidates = [_build_candidate(r) for r in new_records]

    CANDIDATES_PATH.write_text(json.dumps(candidates, indent=2))
    print(f"{len(candidates)} candidate(s) written to {CANDIDATES_PATH}")


if __name__ == "__main__":
    run()
