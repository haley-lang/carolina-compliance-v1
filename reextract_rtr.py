"""
Re-extract RTR Construction LLC certificates using the current production
extractor and update the existing Incoming Extractions rows in Airtable.

Usage (from the project folder, with .env present):
    python reextract_rtr.py "<Original Certificates folder>"           # dry run
    python reextract_rtr.py "<Original Certificates folder>" --apply   # write to Airtable
    python reextract_rtr.py "<folder>" --only-statements               # dry run, statement-only merge
    python reextract_rtr.py "<folder>" --only-statements --apply       # apply statement-only merge

Dry run: shows per-row what would change and a cost estimate. Nothing is written.

--apply:
    Backs up current Raw JSON before overwriting.
    - If "Prior Raw JSON (superseded)" is empty → writes there.
    - If it already has content → writes to "Prior Raw JSON 2 (superseded)" instead,
      so every prior version is preserved. Never overwrites an existing backup.
    Review Status / Review Reason / Processing Status are never touched.
    No rows are created or deleted.

Rows that are skipped (both dry run and apply):
    - Review Status is Approved, Approved + Edited, Rejected, or Escalated to GC
    - Row has at least one Corrections row linked to it (user has already reviewed it)

--only-statements:
    Re-extracts the certificate but only grafts the two statement fields
    (additional_insured_statement, waiver_of_subrogation_statement) from the new
    extraction into the existing Raw JSON. All other existing values are kept
    unchanged. Use this when a prior extraction was done before statement fields
    existed and you only need to fill those in.

    Limitation: the model still reads the whole cert (same token cost as a full
    re-extraction). The statement values in the new extraction may reference
    policies from the new read; since we keep old policy data, minor cross-field
    inconsistency is possible (e.g. statement says "GL policy" but old policy
    number differs). Acceptable for filling blank fields; not acceptable if you
    also need updated policy numbers or dates — run a full re-extraction instead.
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
from review_gate import (
    REVIEW_STATUS_APPROVED,
    REVIEW_STATUS_APPROVED_EDITED,
    REVIEW_STATUS_REJECTED,
    REVIEW_STATUS_ESCALATED,
)

PRIOR_RAW_JSON_FIELD_NAME   = "Prior Raw JSON (superseded)"       # read by field name
PRIOR_RAW_JSON_FIELD_ID     = "fldTGsebc6o2ll5Pu"                # write by ID (rename-safe)
PRIOR_RAW_JSON_2_FIELD_NAME = "Prior Raw JSON 2 (superseded)"     # read by field name
PRIOR_RAW_JSON_2_FIELD_ID   = "fldZCbmIdbJbgbp2E"                # write by ID
SECOND_READ_JSON_FIELD_ID   = "fldJFX9EN6j40cdXD"
AI_DISAGREEMENTS_FIELD_ID   = "fldbiX9zZHdF3RGDQ"
CORRECTIONS_TABLE_ID        = "tblh6NTQikjzl8FFu"
CORRECTIONS_EXTRACTION_ID_FIELD = "Incoming Extraction Record ID"

_SKIP_STATUSES = {
    REVIEW_STATUS_APPROVED,
    REVIEW_STATUS_APPROVED_EDITED,
    REVIEW_STATUS_REJECTED,
    REVIEW_STATUS_ESCALATED,
}

# $ per million tokens for claude-sonnet-5-5
_PRICE_IN, _PRICE_OUT = 2.0, 10.0


def _estimate_cost(in_tok: int, out_tok: int) -> float:
    return (in_tok * _PRICE_IN + out_tok * _PRICE_OUT) / 1_000_000


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", Path(name).stem.lower()).strip("_")


def _build_page_map(folder: Path, work: Path) -> dict:
    """normalized stem → certificate PDF path for every cert in folder."""
    pages = {}
    for pdf in sorted(folder.glob("*")):
        if pdf.suffix.lower() != ".pdf":
            continue
        out = work / _norm(pdf.name)
        out.mkdir(parents=True, exist_ok=True)
        for part in pdf_bundle.certificate_files(pdf, out):
            pages[_norm(Path(part).name)] = Path(part)
    return pages


def _build_refresh_fields(data: dict) -> dict:
    """Fields dict for the Airtable update. Status fields are excluded."""
    second_read = data.get("_second_read")
    ai_disagrees = data.get("_ai_disagreements")
    clean_data = {k: v for k, v in data.items() if k not in ("_second_read", "_ai_disagreements")}

    contact_emails = clean_data.get("contact_emails") or []
    contact_emails_str = (
        ", ".join(str(e) for e in contact_emails)
        if isinstance(contact_emails, list) else str(contact_emails)
    )

    policies = clean_data.get("policies") or []
    policies_count = len(policies) if isinstance(policies, list) else 0

    raw_confidence = clean_data.get("confidence")
    confidence_value = (
        float(raw_confidence)
        if isinstance(raw_confidence, (int, float)) and not isinstance(raw_confidence, bool)
        else None
    )

    second_read_json = json.dumps(second_read, indent=2) if second_read is not None else None
    if isinstance(ai_disagrees, list):
        ai_disagrees_str = "\n".join(ai_disagrees) if ai_disagrees else None
    elif ai_disagrees:
        ai_disagrees_str = str(ai_disagrees)
    else:
        ai_disagrees_str = None

    fields = {
        "Raw JSON": json.dumps(clean_data, indent=2),
        "Document Type": clean_data.get("document_type") or "",
        "Named Insured": clean_data.get("named_insured") or "",
        "Certificate Holder": clean_data.get("certificate_holder") or "",
        "Contact Emails": contact_emails_str,
        "Policies Count": policies_count,
        "Confidence Score": confidence_value,
    }
    if second_read_json is not None:
        fields[SECOND_READ_JSON_FIELD_ID] = second_read_json
    if ai_disagrees_str is not None:
        fields[AI_DISAGREEMENTS_FIELD_ID] = ai_disagrees_str
    return fields


def _fetch_corrected_record_ids(api, base_id: str) -> set:
    """Return set of Incoming Extraction record IDs that have at least one Corrections row."""
    try:
        table = api.table(base_id, CORRECTIONS_TABLE_ID)
        corrected = set()
        for rec in table.all(fields=[CORRECTIONS_EXTRACTION_ID_FIELD]):
            rid = (rec.get("fields") or {}).get(CORRECTIONS_EXTRACTION_ID_FIELD, "")
            if rid:
                corrected.add(rid.strip())
        return corrected
    except Exception as exc:
        print(f"  WARNING: could not fetch Corrections table: {exc}. No rows will be skipped for corrections.")
        return set()


def _decide_backup(f: dict, old_raw: str) -> tuple[str | None, str]:
    """Return (field_id_to_write_backup, human_note) for the backup logic.

    Rules:
    - No existing raw JSON → nothing to back up
    - Prior Raw JSON 1 empty → back up there
    - Prior Raw JSON 1 has content, Prior Raw JSON 2 empty → back up to slot 2
    - Both filled → cannot back up (should not happen in practice)
    """
    if not old_raw:
        return None, "no prior Raw JSON to back up"
    has_backup_1 = bool(f.get(PRIOR_RAW_JSON_FIELD_NAME))
    has_backup_2 = bool(f.get(PRIOR_RAW_JSON_2_FIELD_NAME))
    if not has_backup_1:
        return PRIOR_RAW_JSON_FIELD_ID, "backup → slot 1"
    if not has_backup_2:
        return PRIOR_RAW_JSON_2_FIELD_ID, "backup → slot 2"
    return None, "WARNING: both backup slots full — skipping backup (manual review needed)"


def run(cert_folder: Path, apply: bool, only_statements: bool, table, api, base_id: str) -> dict:
    """Re-extract rows whose Source Filename matches a page in cert_folder.

    Returns a summary dict: matched, skipped_status, skipped_corrections, no_page,
                             succeeded, failed, cost.
    """
    total_cost = 0.0
    matched = skipped_status = skipped_corrections = no_page = succeeded = failed = 0

    print("Fetching rows with Corrections…")
    corrected_ids = _fetch_corrected_record_ids(api, base_id)
    print(f"  {len(corrected_ids)} record(s) have Corrections rows")

    with tempfile.TemporaryDirectory() as tmp:
        pages = _build_page_map(cert_folder, Path(tmp))
        print(f"Split originals into {len(pages)} certificate page(s)")

        for rec in table.all():
            f = rec["fields"]
            source_filename = f.get("Source Filename") or ""
            review_status = (f.get("Review Status") or "").strip()

            path = pages.get(_norm(source_filename))
            if not path:
                no_page += 1
                continue

            # Skip rows in terminal or reviewed statuses
            if review_status in _SKIP_STATUSES:
                skipped_status += 1
                print(f"  SKIP (Status={review_status!r})  {source_filename}")
                continue

            # Skip rows that already have Corrections (user has reviewed them)
            if rec["id"] in corrected_ids:
                skipped_corrections += 1
                print(f"  SKIP (has Corrections)  {source_filename}")
                continue

            matched += 1
            mode_label = "(statements only)" if only_statements else ""
            print(f"  Extracting {source_filename}… {mode_label}")

            try:
                data = extractor.extract_document(path)
                data = extractor.normalize_policy_types(data)
                data = extractor.normalize_policy_dates(data)
                data = extractor.apply_simple_document_classification(data, path)
                data = extractor.drop_empty_policies(data)
            except Exception as exc:
                print(f"    FAILED: {exc}")
                failed += 1
                continue

            total_cost += _estimate_cost(3500, 1200)

            old_raw = f.get("Raw JSON") or ""
            old_data = {}
            try:
                old_data = json.loads(old_raw) if old_raw else {}
            except json.JSONDecodeError:
                pass

            if only_statements:
                # Graft only the two statement fields into the existing data
                new_data = dict(old_data)
                new_data["additional_insured_statement"] = data.get("additional_insured_statement")
                new_data["waiver_of_subrogation_statement"] = data.get("waiver_of_subrogation_statement")
                changes = []
                for stmt_key in ("additional_insured_statement", "waiver_of_subrogation_statement"):
                    old_val = old_data.get(stmt_key)
                    new_val = new_data.get(stmt_key)
                    if old_val != new_val:
                        changes.append(f"    {stmt_key}: {old_val!r} → {new_val!r}")
                new_fields = {
                    "Raw JSON": json.dumps(new_data, indent=2),
                }
            else:
                new_fields = _build_refresh_fields(data)
                changes = []
                for key, new_val in new_fields.items():
                    if key == "Raw JSON":
                        continue
                    old_val = f.get(key)
                    if str(old_val or "") != str(new_val or ""):
                        changes.append(f"    {key}: {old_val!r} → {new_val!r}")
                if old_data != data:
                    changes.append("    Raw JSON: (content differs)")

            backup_field_id, backup_note = _decide_backup(f, old_raw)

            if apply:
                update = dict(new_fields)
                if backup_field_id:
                    update[backup_field_id] = old_raw
                table.update(rec["id"], update, typecast=True)
                succeeded += 1
                print(f"    Updated ({backup_note}, {len(changes)} field(s) changed)")
            else:
                print(f"    DRY RUN ({backup_note}): {len(changes)} field(s) would change")
                for c in changes:
                    print(c)
                succeeded += 1

    print()
    print(f"Matched: {matched}  skipped-status: {skipped_status}  "
          f"skipped-corrections: {skipped_corrections}  no-page: {no_page}  failed: {failed}")
    print(f"Rough cost estimate: ${total_cost:.3f}")
    if not apply:
        print("Dry run only. Add --apply to write to Airtable.")

    return {
        "matched": matched,
        "skipped_status": skipped_status,
        "skipped_corrections": skipped_corrections,
        "no_page": no_page,
        "succeeded": succeeded,
        "failed": failed,
        "cost": total_cost,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("cert_folder", help="path to Original Certificates folder (bundle PDFs)")
    ap.add_argument("--apply", action="store_true",
                    help="write to Airtable (default: dry run, writes nothing)")
    ap.add_argument("--only-statements", action="store_true",
                    help="only graft additional_insured_statement and waiver_of_subrogation_statement "
                         "into the existing Raw JSON; leave all other values unchanged")
    args = ap.parse_args()

    if not extractor.ANTHROPIC_API_KEY:
        sys.exit("ANTHROPIC_API_KEY is not set in .env")
    if not config.AIRTABLE_API_KEY or not config.AIRTABLE_BASE_ID:
        sys.exit("AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env")

    from pyairtable import Api
    token = (config.AIRTABLE_API_KEY or "").strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = Api(token)
    table = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)

    run(Path(args.cert_folder).expanduser(), args.apply, args.only_statements, table, api, base_id)


if __name__ == "__main__":
    main()
