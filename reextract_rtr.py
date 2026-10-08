"""
Re-extract RTR Construction LLC certificates using the current production
extractor (Sonnet 5.5 + checkbox rules) and update the existing Incoming
Extractions rows in Airtable.

Usage (from the project folder, with .env present):
    python reextract_rtr.py "<Original Certificates folder>"          # dry run
    python reextract_rtr.py "<Original Certificates folder>" --apply  # write to Airtable

Dry run (default): shows per-row what would change and a cost estimate.
                   Nothing is written to Airtable.
--apply: backs up current Raw JSON to 'Prior Raw JSON (superseded)'
         (field fldTGsebc6o2ll5Pu) ONLY if that field is still empty,
         then writes the refreshed extraction fields.
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
from review_gate import REVIEW_STATUS_REJECTED

PRIOR_RAW_JSON_FIELD_ID = "fldTGsebc6o2ll5Pu"

# $ per million tokens for claude-sonnet-5-5 (same table as eval_extractor.py)
_PRICE_IN, _PRICE_OUT = 2.0, 10.0


def _estimate_cost(in_tok: int, out_tok: int) -> float:
    return (in_tok * _PRICE_IN + out_tok * _PRICE_OUT) / 1_000_000


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", Path(name).stem.lower()).strip("_")


def _build_page_map(folder: Path, work: Path) -> dict:
    """normalized stem -> split PDF path for every certificate page in folder."""
    pages = {}
    for pdf in sorted(folder.glob("*")):
        if pdf.suffix.lower() != ".pdf":
            continue
        out = work / _norm(pdf.name)
        out.mkdir(parents=True, exist_ok=True)
        for part in pdf_bundle.split_pdf(pdf, out) or []:
            pages[_norm(Path(part).name)] = Path(part)
    return pages


def _build_refresh_fields(data: dict) -> dict:
    """Fields dict for the Airtable update. Status fields are excluded."""
    contact_emails = data.get("contact_emails") or []
    contact_emails_str = (
        ", ".join(str(e) for e in contact_emails)
        if isinstance(contact_emails, list)
        else str(contact_emails)
    )

    policies = data.get("policies") or []
    policies_count = len(policies) if isinstance(policies, list) else 0

    raw_confidence = data.get("confidence")
    confidence_value = (
        float(raw_confidence)
        if isinstance(raw_confidence, (int, float)) and not isinstance(raw_confidence, bool)
        else None
    )

    return {
        "Raw JSON": json.dumps(data, indent=2),
        "Document Type": data.get("document_type") or "",
        "Named Insured": data.get("named_insured") or "",
        "Certificate Holder": data.get("certificate_holder") or "",
        "Contact Emails": contact_emails_str,
        "Policies Count": policies_count,
        "Confidence Score": confidence_value,
    }


def run(cert_folder: Path, apply: bool, table) -> dict:
    """Re-extract rows whose Source Filename matches a page in cert_folder.

    Injects table so the function is testable without a live Airtable connection.
    Returns a summary dict with keys: matched, rejected, no_page, succeeded, failed, cost.
    """
    total_cost = 0.0
    matched = rejected = no_page = succeeded = failed = 0

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

            if review_status == REVIEW_STATUS_REJECTED:
                rejected += 1
                print(f"  SKIP (Rejected)  {source_filename}")
                continue

            matched += 1
            print(f"  Extracting {source_filename}…")

            try:
                data = extractor.extract_document(path)
                data = extractor.normalize_policy_dates(data)
                data = extractor.apply_simple_document_classification(data, path)
                data = extractor.drop_empty_policies(data)
            except Exception as exc:
                print(f"    FAILED: {exc}")
                failed += 1
                continue

            total_cost += _estimate_cost(3500, 1200)
            new_fields = _build_refresh_fields(data)
            old_raw = f.get("Raw JSON") or ""
            has_backup = bool(f.get(PRIOR_RAW_JSON_FIELD_ID))

            if apply:
                update = dict(new_fields)
                if not has_backup and old_raw:
                    update[PRIOR_RAW_JSON_FIELD_ID] = old_raw
                    backup_note = "backup written"
                elif has_backup:
                    backup_note = "backup already present — skipped"
                else:
                    backup_note = "no prior Raw JSON to back up"
                table.update(rec["id"], update, typecast=True)
                succeeded += 1
                print(f"    Updated ({backup_note})")
            else:
                # Dry run: show what would change
                old_data = {}
                try:
                    old_data = json.loads(old_raw) if old_raw else {}
                except json.JSONDecodeError:
                    pass
                changes = []
                for key, new_val in new_fields.items():
                    if key == "Raw JSON":
                        continue  # compare as parsed dicts below
                    old_val = f.get(key)
                    if str(old_val or "") != str(new_val or ""):
                        changes.append(f"    {key}: {old_val!r} → {new_val!r}")
                if old_data != data:
                    changes.append("    Raw JSON: (content differs)")
                if not has_backup and old_raw:
                    backup_note = "backup would be written"
                elif has_backup:
                    backup_note = "backup already present"
                else:
                    backup_note = "no prior Raw JSON"
                print(f"    DRY RUN ({backup_note}): {len(changes)} field(s) would change")
                for c in changes:
                    print(c)
                succeeded += 1

    print()
    print(f"Matched: {matched}  skipped (Rejected): {rejected}  "
          f"no page match: {no_page}  failed: {failed}")
    print(f"Rough cost estimate: ${total_cost:.3f}")
    if not apply:
        print("Dry run only. Add --apply to write to Airtable.")

    return {
        "matched": matched,
        "rejected": rejected,
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
    args = ap.parse_args()

    if not extractor.ANTHROPIC_API_KEY:
        sys.exit("ANTHROPIC_API_KEY is not set in .env")
    if not config.AIRTABLE_API_KEY or not config.AIRTABLE_BASE_ID:
        sys.exit("AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env")

    from pyairtable import Api
    token = (config.AIRTABLE_API_KEY or "").strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    table = Api(token).table(base_id, INCOMING_EXTRACTIONS_TABLE)

    run(Path(args.cert_folder).expanduser(), args.apply, table)


if __name__ == "__main__":
    main()
