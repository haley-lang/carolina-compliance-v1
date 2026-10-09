"""Apply the phantom-row cleanup fix to existing RTR Incoming Extractions rows.

Does NOT call the Anthropic API. Re-parses each row's existing Raw JSON, re-runs
drop_empty_policies (which now catches $0/blank-limits phantom rows and duplicate-
number phantom rows), and shows or writes the diff.

Only rows whose policy list actually changes are touched — unaffected rows are
left completely alone.

Usage (from the project folder, with .env present):
    python apply_phantom_fix_rtr.py "<Original Certificates folder>"          # dry run
    python apply_phantom_fix_rtr.py "<Original Certificates folder>" --apply  # write

Dry run (default): prints which rows would change, what gets dropped.
--apply:  backs up current Raw JSON to 'Prior Raw JSON (superseded)'
          (field fldTGsebc6o2ll5Pu) ONLY if that field is still empty,
          then writes updated Raw JSON and Policies Count.
          Review Status / Review Reason / Processing Status are never touched.
          No rows are created or deleted.
"""
import argparse
import json
import re
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import config
import extractor
import pdf_bundle
from airtable_importer import INCOMING_EXTRACTIONS_TABLE, clean_base_id

PRIOR_RAW_JSON_FIELD_NAME = "Prior Raw JSON (superseded)"
PRIOR_RAW_JSON_FIELD_ID   = "fldTGsebc6o2ll5Pu"


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", Path(name).stem.lower()).strip("_")


def _collect_rtr_stems(folder: Path) -> set:
    """Return normalized stems for every certificate page split from folder."""
    stems = set()
    with tempfile.TemporaryDirectory() as tmp:
        for pdf in sorted(folder.glob("*")):
            if pdf.suffix.lower() != ".pdf":
                continue
            out = Path(tmp) / _norm(pdf.name)
            out.mkdir(parents=True, exist_ok=True)
            for part in pdf_bundle.split_pdf(pdf, out) or []:
                stems.add(_norm(Path(part).name))
    return stems


def run(cert_folder: Path, apply: bool, table) -> dict:
    """Main logic; table is injected so tests can pass a mock."""
    rtr_stems = _collect_rtr_stems(cert_folder)
    print(f"Resolved {len(rtr_stems)} RTR page stem(s) from {cert_folder}")

    matched = changed = skipped = no_match = 0

    for rec in table.all():
        f = rec["fields"]
        source_filename = f.get("Source Filename") or ""

        if _norm(source_filename) not in rtr_stems:
            no_match += 1
            continue
        matched += 1

        old_raw = f.get("Raw JSON") or ""
        try:
            data = json.loads(old_raw) if old_raw else {}
        except json.JSONDecodeError:
            print(f"  SKIP (unparseable Raw JSON): {source_filename}")
            skipped += 1
            continue

        old_policies = list(data.get("policies") or [])
        data = extractor.drop_empty_policies(data)
        new_policies = data.get("policies") or []

        if len(new_policies) == len(old_policies):
            continue  # nothing changed for this row

        changed += 1
        dropped = [p.get("policy_type", "?") for p in old_policies if p not in new_policies]
        print(f"  {source_filename}: {len(old_policies)} → {len(new_policies)} polic(ies)  "
              f"dropped: {dropped}")

        if apply:
            new_raw = json.dumps(data, indent=2)
            update = {
                "Raw JSON": new_raw,
                "Policies Count": len(new_policies),
            }
            has_backup = bool(f.get(PRIOR_RAW_JSON_FIELD_NAME))
            if not has_backup and old_raw:
                update[PRIOR_RAW_JSON_FIELD_ID] = old_raw
                backup_note = "backup written"
            elif has_backup:
                backup_note = "backup already present — skipped"
            else:
                backup_note = "no prior Raw JSON to back up"
            table.update(rec["id"], update, typecast=True)
            print(f"    → updated  ({backup_note})")

    print()
    print(f"RTR rows: {matched} matched, {changed} would change, "
          f"{skipped} skipped (bad JSON), {no_match} not RTR.")
    if not apply:
        print("Dry run only. Add --apply to write to Airtable.")

    return {"matched": matched, "changed": changed, "skipped": skipped, "no_match": no_match}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("cert_folder",
                    help="Path to the RTR Original Certificates folder (bundle PDFs)")
    ap.add_argument("--apply", action="store_true",
                    help="Write changes to Airtable (default: dry run, nothing written)")
    args = ap.parse_args()

    if not config.AIRTABLE_API_KEY or not config.AIRTABLE_BASE_ID:
        sys.exit("AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env")

    from pyairtable import Api
    token = (config.AIRTABLE_API_KEY or "").strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    table = Api(token).table(base_id, INCOMING_EXTRACTIONS_TABLE)

    run(Path(args.cert_folder).expanduser(), args.apply, table)


if __name__ == "__main__":
    main()
