#!/usr/bin/env python3
"""
post_confirm_processor.py

After a reviewer approves an extraction on the review screen (Review Status =
"Approved" or "Approved + Edited"), this script completes the job:

  1. Match or create a Vendor (suffix-normalized, fuzzy-flagged)
  2. Create a Vendor Client Assignment linking the vendor to RTR
  3. Set Processing Status = "Imported"
  4. Run processor.py as a subprocess to create Insurance Policies + Certificates

Designed for RTR Construction LLC (client recG2hxGoJKdgBr4M). Skips any
extraction whose Certificate Holder doesn't contain "rtr" to prevent
accidentally creating VCAs for the wrong client.

Modes:
  --dry-run          Scan all unprocessed RTR extractions; print vendor plan. No writes.
  --apply            Process all currently Approved/Approved+Edited extractions.
  --apply --record ID  Process a single extraction by Airtable record ID.
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config  # noqa: E402
from processor import clean_base_id, create_vendor
from incoming_extraction_matcher_slice import clean_named_insured
from pyairtable import Api

SCRIPT_DIR = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

RTR_CLIENT_ID = "recG2hxGoJKdgBr4M"

REVIEW_STATUS_APPROVED = "Approved"
REVIEW_STATUS_APPROVED_EDITED = "Approved + Edited"
PROCESSING_STATUS_IMPORTED = "Imported"

POSSIBLE_MATCH_THRESHOLD = 75  # fuzzy score below which we flag instead of auto-create

# Legal suffixes to strip before comparing company names
_SUFFIXES = [
    "limited liability company",
    "limited liability partnership",
    "l.l.c.", "l.l.p.", "p.l.l.c.",
    "llc", "llp", "pllc",
    "incorporated", "corporation", "company", "limited",
    "inc.", "corp.", "ltd.", "co.",
    "inc", "corp", "ltd", "co",
    "lp", "l.p.",
]

# RTR's own names — extractions where the insured IS RTR, not a subcontractor
_RTR_SELF_NAMES = {
    "rtr renovations",
    "rtr renovations llc",
    "rtr construction",
    "rtr construction llc",
}


def normalize_for_match(name: str) -> str:
    """Lowercase, remove punctuation, strip legal suffixes for comparison."""
    text = name.strip().lower()
    text = text.replace(",", " ").replace(".", " ").replace("'", "").replace("-", " ")
    text = " ".join(text.split())
    changed = True
    while changed:
        changed = False
        for suffix in sorted(_SUFFIXES, key=len, reverse=True):
            if text.endswith(" " + suffix):
                text = text[: -len(" " + suffix)].strip()
                changed = True
                break
    return " ".join(text.split())


def is_rtr_self(named_insured: str) -> bool:
    return normalize_for_match(named_insured) in _RTR_SELF_NAMES


def is_rtr_cert(extraction_fields: dict) -> bool:
    cert_holder = (extraction_fields.get("Certificate Holder") or "").lower()
    return "rtr" in cert_holder


def fuzzy_score(a: str, b: str) -> int:
    try:
        from rapidfuzz import fuzz
        return int(fuzz.ratio(a, b))
    except ImportError:
        return 0


# ── Vendor resolution ─────────────────────────────────────────────────────────

def plan_vendor_action(named_insured: str, vendor_pool: list) -> dict:
    """Decide how to handle a named insured against a pool of existing vendors.

    Returns:
        action: "skip_client" | "skip_blank" | "exact_match" | "alias_match"
                | "suffix_match" | "possible_match" | "create"
        vendor_id: existing vendor ID (None if creating)
        vendor_name: stored vendor name (or the name we'd use for create)
        note: human-readable explanation
    """
    cleaned = clean_named_insured(named_insured).strip()
    if not cleaned:
        return {"action": "skip_blank", "vendor_id": None, "vendor_name": "", "note": "blank"}

    if is_rtr_self(cleaned):
        return {"action": "skip_client", "vendor_id": None, "vendor_name": cleaned, "note": "RTR itself"}

    needle_lower = cleaned.lower()
    needle_norm = normalize_for_match(cleaned)

    # 1. Case-insensitive exact match on Vendor Name
    for v in vendor_pool:
        vname = (v.get("fields", {}).get("Vendor Name") or "").strip()
        if vname.lower() == needle_lower:
            return {"action": "exact_match", "vendor_id": v["id"], "vendor_name": vname, "note": "exact"}

    # 2. Alias match
    for v in vendor_pool:
        vf = v.get("fields", {})
        aliases_raw = vf.get("Aliases") or ""
        if isinstance(aliases_raw, str):
            for alias in (a.strip() for a in aliases_raw.split(",") if a.strip()):
                if alias.lower() == needle_lower:
                    return {
                        "action": "alias_match",
                        "vendor_id": v["id"],
                        "vendor_name": vf.get("Vendor Name", ""),
                        "note": f"alias '{alias}'",
                    }

    # 3. Suffix-normalized exact match (e.g. "Magic Monkey LLC" vs "Magic Monkey")
    if needle_norm:
        for v in vendor_pool:
            vname = (v.get("fields", {}).get("Vendor Name") or "").strip()
            if normalize_for_match(vname) == needle_norm:
                return {
                    "action": "suffix_match",
                    "vendor_id": v["id"],
                    "vendor_name": vname,
                    "note": f"suffix-normalized: '{cleaned}' ~ '{vname}'",
                }

    # 4. Fuzzy check (flag; do not auto-create)
    best_score = 0
    best_vendor = None
    for v in vendor_pool:
        vname = (v.get("fields", {}).get("Vendor Name") or "").strip()
        score = max(
            fuzzy_score(needle_lower, vname.lower()),
            fuzzy_score(needle_norm, normalize_for_match(vname)),
        )
        if score > best_score:
            best_score = score
            best_vendor = v

    if best_score >= POSSIBLE_MATCH_THRESHOLD and best_vendor:
        bname = (best_vendor.get("fields", {}).get("Vendor Name") or "")
        return {
            "action": "possible_match",
            "vendor_id": best_vendor["id"],
            "vendor_name": bname,
            "note": f"fuzzy {best_score}% — needs review",
        }

    return {"action": "create", "vendor_id": None, "vendor_name": cleaned, "note": "new"}


# ── VCA helpers ───────────────────────────────────────────────────────────────

def find_existing_vca(vca_table, vendor_id: str, client_id: str):
    safe_vendor = vendor_id.replace("'", "\\'")
    safe_client = client_id.replace("'", "\\'")
    formula = (
        f"AND("
        f"FIND('{safe_vendor}', ARRAYJOIN({{Vendor Link}})), "
        f"FIND('{safe_client}', ARRAYJOIN({{Client Link}}))"
        f")"
    )
    return vca_table.first(formula=formula)


def create_vca(vca_table, vendor_id: str, client_id: str):
    record = vca_table.create(
        {"Vendor Link": [vendor_id], "Client Link": [client_id], "Active": True},
        typecast=True,
    )
    logger.info("Created VCA %s  vendor=%s client=%s", record["id"], vendor_id, client_id)
    return record


# ── Dry run ───────────────────────────────────────────────────────────────────

def run_dry_run(api, base_id: str) -> None:
    incoming_table = api.table(base_id, "Incoming Extractions")
    vendors_table = api.table(base_id, "Vendors")

    print("\n=== DRY RUN — RTR Vendor Plan ===\n")
    print("Fetching all Incoming Extractions...")
    all_extractions = incoming_table.all()

    rtr_extractions = [
        rec for rec in all_extractions
        if is_rtr_cert(rec.get("fields", {}))
    ]
    print(f"  {len(all_extractions)} total extractions, {len(rtr_extractions)} with 'RTR' cert holder\n")

    existing_vendors = vendors_table.all()
    print(f"  {len(existing_vendors)} existing vendors in base\n")

    # Deduplicate by cleaned Named Insured (case-insensitive)
    name_map: dict[str, list] = {}
    for rec in rtr_extractions:
        f = rec.get("fields", {})
        raw = f.get("Named Insured") or ""
        cleaned = clean_named_insured(raw).strip()
        key = cleaned.lower() if cleaned else "(blank)"
        name_map.setdefault(key, []).append({
            "record_id": rec["id"],
            "named_insured": cleaned,
            "review_status": f.get("Review Status") or "—",
            "processing_status": f.get("Processing Status") or "—",
        })

    print(f"  {len(name_map)} distinct Named Insureds\n")
    print(f"  {'Named Insured':<45} {'Action':<16} {'Note':<52} Extractions")
    print("  " + "-" * 125)

    # Simulate creates so later rows see the same vendor pool we'd actually have
    simulated_pool = list(existing_vendors)
    counts: dict[str, int] = {}

    for key, recs in sorted(name_map.items()):
        named_insured = recs[0]["named_insured"] or key
        plan = plan_vendor_action(named_insured, simulated_pool)
        action = plan["action"]
        counts[action] = counts.get(action, 0) + 1

        if action == "create":
            simulated_pool.append({
                "id": f"(new)",
                "fields": {"Vendor Name": named_insured, "Aliases": ""},
            })

        ext_list = "  ".join(
            f"{r['record_id'][:8]}({r['review_status'][:10]})" for r in recs
        )
        print(f"  {named_insured[:43]:<45} {action:<16} {plan['note'][:50]:<52} {ext_list}")

    print("\n  --- Summary ---")
    for action, count in sorted(counts.items()):
        print(f"    {action:<20}: {count}")

    possible_matches = [
        (key, recs)
        for key, recs in name_map.items()
        if plan_vendor_action(recs[0]["named_insured"] or key, existing_vendors)["action"] == "possible_match"
    ]
    if possible_matches:
        print("\n  --- Possible Matches (need review before apply) ---")
        for key, recs in possible_matches:
            ni = recs[0]["named_insured"] or key
            p = plan_vendor_action(ni, existing_vendors)
            print(f"    '{ni}' → possible match with '{p['vendor_name']}' ({p['note']})")

    print("\nRun with --apply to process all Approved/Approved+Edited extractions.\n")


# ── Apply ─────────────────────────────────────────────────────────────────────

def run_apply(api, base_id: str, record_ids: list | None = None) -> None:
    incoming_table = api.table(base_id, "Incoming Extractions")
    vendors_table = api.table(base_id, "Vendors")
    vca_table = api.table(base_id, "Vendor Client Assignments")

    if record_ids:
        records_to_process = [incoming_table.get(rid) for rid in record_ids]
    else:
        all_extractions = incoming_table.all()
        records_to_process = [
            rec for rec in all_extractions
            if (rec.get("fields", {}).get("Review Status") or "")
            in (REVIEW_STATUS_APPROVED, REVIEW_STATUS_APPROVED_EDITED)
        ]

    if not records_to_process:
        print("No approved extractions found.")
        return

    print(f"\n=== APPLY — {len(records_to_process)} extraction(s) ===\n")

    vendors_created: list[dict] = []
    vendors_matched: list[dict] = []
    vcas_created: list[dict] = []
    vcas_existing: list[dict] = []
    possible_matches: list[dict] = []
    skipped: list[dict] = []
    set_imported: list[str] = []
    errors: list[dict] = []

    for rec in records_to_process:
        extraction_id = rec["id"]
        fields = rec.get("fields", {})
        raw_ni = fields.get("Named Insured") or ""
        named_insured = clean_named_insured(raw_ni).strip()
        review_status = fields.get("Review Status") or ""
        proc_status = fields.get("Processing Status") or ""

        print(f"[{extraction_id}] '{named_insured or '(blank)'}'"
              f"  review={review_status}  proc={proc_status}")

        if not named_insured:
            print("  SKIP: blank Named Insured")
            skipped.append({"id": extraction_id, "reason": "blank"})
            continue

        if is_rtr_self(named_insured):
            print("  SKIP: named insured is RTR itself")
            skipped.append({"id": extraction_id, "reason": "RTR self"})
            continue

        # Re-fetch vendor pool each iteration so we see vendors we just created
        existing_vendors = vendors_table.all()
        plan = plan_vendor_action(named_insured, existing_vendors)
        action = plan["action"]

        if action == "possible_match":
            print(f"  POSSIBLE MATCH: '{named_insured}' ~ '{plan['vendor_name']}' ({plan['note']})")
            print("  Skipping — confirm the right vendor in Airtable, then re-run with --record")
            possible_matches.append({
                "id": extraction_id,
                "named_insured": named_insured,
                "match_name": plan["vendor_name"],
                "note": plan["note"],
            })
            continue

        vendor_id = plan.get("vendor_id")

        if action == "create":
            print(f"  Creating vendor: '{named_insured}'")
            try:
                vendor_rec = create_vendor(vendors_table, named_insured)
                vendor_id = vendor_rec["id"]
                vendors_created.append({"id": vendor_id, "name": named_insured})
                print(f"  Vendor created: {vendor_id}")
            except Exception as exc:
                print(f"  ERROR creating vendor: {exc}")
                errors.append({"id": extraction_id, "step": "create_vendor", "error": str(exc)})
                continue

        elif action == "suffix_match":
            print(f"  Suffix match → vendor {vendor_id} ('{plan['vendor_name']}')")
            # Add the extraction's named insured as an alias for future exact matches
            existing_v = next((v for v in existing_vendors if v["id"] == vendor_id), None)
            if existing_v:
                vf = existing_v.get("fields", {})
                cur_aliases = (vf.get("Aliases") or "").strip()
                existing_alias_set = {
                    a.strip().lower() for a in cur_aliases.split(",") if a.strip()
                }
                if named_insured.lower() not in existing_alias_set:
                    new_aliases = (cur_aliases + ", " + named_insured).strip(", ")
                    try:
                        vendors_table.update(vendor_id, {"Aliases": new_aliases}, typecast=True)
                        print(f"  Added alias '{named_insured}' to vendor {vendor_id}")
                    except Exception as exc:
                        logger.warning("Could not add alias: %s", exc)
            vendors_matched.append({"id": vendor_id, "name": plan["vendor_name"], "via": action})

        else:  # exact_match or alias_match
            print(f"  {action} → vendor {vendor_id} ('{plan['vendor_name']}')")
            vendors_matched.append({"id": vendor_id, "name": plan["vendor_name"], "via": action})

        if not vendor_id:
            print("  ERROR: no vendor_id after resolution")
            errors.append({"id": extraction_id, "step": "vendor_resolution", "error": "no vendor_id"})
            continue

        # VCA
        try:
            existing_vca = find_existing_vca(vca_table, vendor_id, RTR_CLIENT_ID)
        except Exception as exc:
            logger.warning("VCA lookup failed: %s — will attempt create", exc)
            existing_vca = None

        if existing_vca:
            print(f"  VCA already exists: {existing_vca['id']}")
            vcas_existing.append({"vendor_id": vendor_id, "vca_id": existing_vca["id"]})
        else:
            try:
                vca_rec = create_vca(vca_table, vendor_id, RTR_CLIENT_ID)
                vcas_created.append({"vendor_id": vendor_id, "vca_id": vca_rec["id"]})
                print(f"  VCA created: {vca_rec['id']}")
            except Exception as exc:
                print(f"  ERROR creating VCA: {exc}")
                errors.append({"id": extraction_id, "step": "create_vca", "error": str(exc)})
                continue

        # Set Processing Status = Imported
        try:
            incoming_table.update(
                extraction_id,
                {"Processing Status": PROCESSING_STATUS_IMPORTED},
                typecast=True,
            )
            set_imported.append(extraction_id)
            print("  Processing Status → Imported")
        except Exception as exc:
            print(f"  ERROR setting Imported: {exc}")
            errors.append({"id": extraction_id, "step": "set_imported", "error": str(exc)})
            continue

    # Run processor.py once after all extractions are staged
    if set_imported:
        print(f"\nRunning processor.py for {len(set_imported)} Imported extraction(s)...")
        try:
            result = subprocess.run(
                [str(SCRIPT_DIR / ".venv" / "bin" / "python"), "processor.py"],
                cwd=str(SCRIPT_DIR),
            )
            if result.returncode != 0:
                print(f"processor.py exited with code {result.returncode}")
                errors.append({"step": "run_processor", "error": f"exit code {result.returncode}"})
            else:
                print("processor.py completed successfully")
        except Exception as exc:
            print(f"ERROR running processor.py: {exc}")
            errors.append({"step": "run_processor", "error": str(exc)})
    else:
        print("\nNo extractions staged — skipping processor.py")

    # Summary
    print("\n=== Summary ===")
    print(f"  Vendors created    : {len(vendors_created)}")
    for v in vendors_created:
        print(f"    {v['name']} ({v['id']})")
    print(f"  Vendors matched    : {len(vendors_matched)}")
    print(f"  VCAs created       : {len(vcas_created)}")
    print(f"  VCAs existing      : {len(vcas_existing)}")
    print(f"  Set Imported       : {len(set_imported)}")
    if possible_matches:
        print(f"  Possible matches   : {len(possible_matches)}")
        for pm in possible_matches:
            print(f"    '{pm['named_insured']}' ~ '{pm['match_name']}' ({pm['note']})")
    if skipped:
        print(f"  Skipped            : {len(skipped)}")
        for s in skipped:
            print(f"    {s['id'][:8]}: {s['reason']}")
    if errors:
        print(f"  Errors             : {len(errors)}")
        for e in errors:
            print(f"    {e}")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Post-confirm processor: vendor + VCA creation for approved COI extractions"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true",
                      help="Print vendor plan for all RTR extractions; no writes")
    mode.add_argument("--apply", action="store_true",
                      help="Process all Approved/Approved+Edited extractions")
    parser.add_argument("--record", metavar="ID", action="append", dest="records",
                        help="Limit --apply to specific record IDs (repeatable)")
    args = parser.parse_args()

    if args.dry_run and args.records:
        parser.error("--record is not valid with --dry-run")

    api_key = (config.AIRTABLE_API_KEY or "").strip()
    raw_base = (config.AIRTABLE_BASE_ID or "").strip()
    if not api_key or not raw_base:
        sys.exit("ERROR: AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env")

    base_id = clean_base_id(raw_base)
    api = Api(api_key)

    if args.dry_run:
        run_dry_run(api, base_id)
    else:
        run_apply(api, base_id, record_ids=args.records)


if __name__ == "__main__":
    main()
