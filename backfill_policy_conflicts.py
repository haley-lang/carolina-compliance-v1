"""Backfill shared-policy-number conflicts into Incoming Extractions.

Run with:
    .venv/bin/python backfill_policy_conflicts.py

For each known vendor, finds policies with the same normalized policy number
but different types, checks if not already accepted in Accepted Policy Patterns,
then queues the most recent Approved/Approved+Edited extraction for that vendor
into Pending Review with a _shared_policy_conflicts payload.

Also handles special one-off cases (N9WC394833 with no vendor link, 900 Electric,
The Glass Company duplicate Umbrella).
"""
import json
import os
import re
import sys
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

try:
    from pyairtable import Api
except ImportError:
    print("ERROR: pyairtable not installed. Run: .venv/bin/pip install pyairtable")
    sys.exit(1)

# ── Config ───────────────────────────────────────────────────────────────────

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID_RAW = os.environ.get("AIRTABLE_BASE_ID", "").strip()

if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID_RAW:
    print("ERROR: AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env")
    sys.exit(1)

# Strip any table suffix (appXXX/tblYYY → appXXX)
AIRTABLE_BASE_ID = re.sub(r"/tbl.*", "", AIRTABLE_BASE_ID_RAW.strip())

INSURANCE_POLICIES_TABLE_ID        = "tblpPcmm5ANE0bMNB"
INCOMING_EXTRACTIONS_TABLE_ID      = "tblT88Ty6d6M766oY"
ACCEPTED_POLICY_PATTERNS_TABLE_ID  = "tblnrj457WdtRhkei"

APPROVED_STATUSES = {"Approved", "Approved + Edited"}

# ── Known conflict cases ─────────────────────────────────────────────────────

KNOWN_CASES = [
    ("recpB8baBB7DVLzEI", "Romero Team Construction LLC"),
    ("rech1NOxs76VKUUtb", "James Doster, Inc."),
    ("reciwvm4nbkZdM4Fj", "Keys Trucking LLC"),
    ("recSNO9WDtTuaMVd9", "The Glass Company, Inc."),
    ("rec5EKkohptgts4bG", "TruTeam Builder Services Group, Inc."),
    ("recAkU4yKp7XYgwWG", "John Pridgeon JKP Tree Services"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm_polnum(v: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())


VENDORS_TABLE_ID = "tblsOphSd5DKSZEro"


def _get_vendor_policies(api, vendor_record_id: str) -> list:
    # Look up policy IDs via the vendor record's linked Insurance Policies field
    vrec = api.table(AIRTABLE_BASE_ID, VENDORS_TABLE_ID).get(vendor_record_id)
    policy_ids = vrec["fields"].get("Insurance Policies") or []
    if not policy_ids:
        return []
    tbl = api.table(AIRTABLE_BASE_ID, INSURANCE_POLICIES_TABLE_ID)
    formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in policy_ids) + ")"
    records = tbl.all(
        formula=formula,
        fields=["Policy Type", "Policy Number"],
    )
    result = []
    for rec in records:
        pf = rec["fields"]
        result.append({
            "record_id": rec["id"],
            "policy_type": pf.get("Policy Type") or "",
            "policy_number": pf.get("Policy Number") or "",
        })
    return result


def _is_accepted(api, vendor_id: str, norm_num: str) -> bool:
    tbl = api.table(AIRTABLE_BASE_ID, ACCEPTED_POLICY_PATTERNS_TABLE_ID)
    formula = (
        f"AND({{Vendor Record ID}} = '{vendor_id}', "
        f"{{Policy Number Normalized}} = '{norm_num}')"
    )
    recs = tbl.all(formula=formula, max_records=1)
    return bool(recs)


def _find_best_extraction(api, vendor_id: str) -> dict | None:
    """Return the most recent Approved/Approved+Edited extraction for this vendor."""
    # Use vendor record's linked Incoming Extractions field to avoid ARRAYJOIN formula issue
    vrec = api.table(AIRTABLE_BASE_ID, VENDORS_TABLE_ID).get(vendor_id)
    ext_ids = vrec["fields"].get("Incoming Extractions") or []
    if not ext_ids:
        return None
    tbl = api.table(AIRTABLE_BASE_ID, INCOMING_EXTRACTIONS_TABLE_ID)
    formula = "OR(" + ", ".join(f"RECORD_ID()='{e}'" for e in ext_ids) + ")"
    recs = tbl.all(
        formula=formula,
        fields=["Review Status", "Extraction Processed At", "Raw JSON"],
        sort=["-Extraction Processed At"],
    )
    best = None
    for rec in recs:
        status = (rec["fields"].get("Review Status") or "").strip()
        if status in APPROVED_STATUSES:
            if best is None or (rec["fields"].get("Extraction Processed At") or "") > (best["fields"].get("Extraction Processed At") or ""):
                best = rec
    return best


def _queue_extraction(api, extraction_rec: dict, conflicts: list, vendor_name: str) -> str:
    """Store _shared_policy_conflicts in Raw JSON, set Pending Review. Returns record ID."""
    record_id = extraction_rec["id"]
    tbl = api.table(AIRTABLE_BASE_ID, INCOMING_EXTRACTIONS_TABLE_ID)
    try:
        raw_json = json.loads(extraction_rec["fields"].get("Raw JSON") or "{}")
    except json.JSONDecodeError:
        raw_json = {}

    raw_json["_shared_policy_conflicts"] = conflicts
    tbl.update(record_id, {
        "Raw JSON": json.dumps(raw_json, indent=2),
        "Review Status": "Pending Review",
        "Review Reason": "Shared policy number",
    }, typecast=True)
    return record_id


def _queue_extraction_note(api, extraction_rec: dict, note: str) -> str:
    """Add _review_note to Raw JSON, set Pending Review. Returns record ID."""
    record_id = extraction_rec["id"]
    tbl = api.table(AIRTABLE_BASE_ID, INCOMING_EXTRACTIONS_TABLE_ID)
    try:
        raw_json = json.loads(extraction_rec["fields"].get("Raw JSON") or "{}")
    except json.JSONDecodeError:
        raw_json = {}

    raw_json["_review_note"] = note
    tbl.update(record_id, {
        "Raw JSON": json.dumps(raw_json, indent=2),
        "Review Status": "Pending Review",
        "Review Reason": "Shared policy number",
    }, typecast=True)
    return record_id


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    api = Api(AIRTABLE_API_KEY)
    ie_tbl = api.table(AIRTABLE_BASE_ID, INCOMING_EXTRACTIONS_TABLE_ID)

    summary = []  # (vendor, norm_num, types, queued_extraction_id)

    print("\n=== Checking known conflict cases ===\n")

    for vendor_id, vendor_name in KNOWN_CASES:
        print(f"--- {vendor_name} ({vendor_id}) ---")
        policies = _get_vendor_policies(api, vendor_id)

        if not policies:
            print("  No policies found.\n")
            continue

        # Group by normalized policy number → set of types
        num_to_types: dict = defaultdict(set)
        num_to_raw: dict = {}
        for p in policies:
            raw_num = p["policy_number"].strip()
            pol_type = p["policy_type"].strip()
            if not raw_num or not pol_type:
                continue
            norm = _norm_polnum(raw_num)
            if norm:
                num_to_types[norm].add(pol_type)
                if norm not in num_to_raw:
                    num_to_raw[norm] = raw_num

        print(f"  Policies: {len(policies)}")
        for norm, types in sorted(num_to_types.items()):
            raw = num_to_raw.get(norm, norm)
            print(f"  {norm} ({raw}) → {sorted(types)}")

        conflicts = []
        for norm_num, types in num_to_types.items():
            if len(types) < 2:
                continue

            # Special case: The Glass Company CXS 8668541 — same type (Umbrella × 2),
            # not a cross-type conflict. Handle separately below.
            if vendor_name == "The Glass Company, Inc." and norm_num == _norm_polnum("CXS 8668541"):
                print(f"  [SKIP cross-type for CXS 8668541 — handled as duplicate check below]")
                continue

            accepted = _is_accepted(api, vendor_id, norm_num)
            if accepted:
                print(f"  {norm_num}: already in Accepted Policy Patterns — skip")
                continue

            conflicts.append({
                "norm_number": norm_num,
                "raw_number": num_to_raw.get(norm_num, norm_num),
                "types": sorted(types),
            })

        if conflicts:
            ext_rec = _find_best_extraction(api, vendor_id)
            if ext_rec:
                queued_id = _queue_extraction(api, ext_rec, conflicts, vendor_name)
                print(f"  Queued: extraction {queued_id}")
                for c in conflicts:
                    summary.append((vendor_name, c["norm_number"], c["types"], queued_id))
            else:
                print(f"  No approved extraction found to queue.")
        else:
            print("  No unaccepted cross-type conflicts.")

        print()

    # ── Special queue items ──────────────────────────────────────────────────
    print("=== Special queue items ===\n")

    # 1. N9WC394833 — WC policy with no vendor link
    print("--- N9WC394833 (no vendor link) ---")
    recs_n9wc = ie_tbl.all(
        formula="OR(FIND('N9WC394833', {Source Filename}), FIND('N9WC394833', ARRAYJOIN({Raw JSON}, '')))",
        fields=["Review Status", "Raw JSON", "Source Filename"],
        max_records=10,
    )
    if not recs_n9wc:
        print("  No extraction found for N9WC394833 — trying Raw JSON search...")
        recs_n9wc = ie_tbl.all(
            formula="FIND('N9WC394833', {Source Filename})",
            fields=["Review Status", "Raw JSON", "Source Filename"],
            max_records=10,
        )
    for rec in recs_n9wc:
        note = "WC policy N9WC394833 has no vendor link — check vendor match"
        queued_id = _queue_extraction_note(api, rec, note)
        print(f"  Queued: {queued_id} ({rec['fields'].get('Source Filename', '')})")
        summary.append(("N9WC394833 (no vendor)", "N9WC394833", ["Workers' Comp"], queued_id))
    if not recs_n9wc:
        print("  Not found in Incoming Extractions.")
    print()

    # 2. 900 Electric — no vendor record
    print("--- 900 Electric (no vendor record) ---")
    recs_900 = ie_tbl.all(
        formula="FIND('900 Electric', {Named Insured})",
        fields=["Review Status", "Raw JSON", "Named Insured", "Source Filename"],
        max_records=10,
    )
    for rec in recs_900:
        note = "Named insured '900 Electric' has no vendor record in Airtable — check if vendor needs to be created"
        queued_id = _queue_extraction_note(api, rec, note)
        print(f"  Queued: {queued_id} ({rec['fields'].get('Named Insured', '')} / {rec['fields'].get('Source Filename', '')})")
        summary.append(("900 Electric", "—", ["(no vendor)"], queued_id))
    if not recs_900:
        print("  Not found in Incoming Extractions.")
    print()

    # 3. The Glass Company — CXS 8668541 (Umbrella × 2 = potential duplicate, not cross-type)
    print("--- The Glass Company — CXS 8668541 (duplicate Umbrella check) ---")
    glass_id = "recSNO9WDtTuaMVd9"
    glass_ext = _find_best_extraction(api, glass_id)
    if glass_ext:
        note = (
            "Policy CXS 8668541 appears in two Umbrella terms — "
            "check if these should be linked or if one is superseded"
        )
        queued_id = _queue_extraction_note(api, glass_ext, note)
        print(f"  Queued: {queued_id}")
        summary.append(("The Glass Company, Inc.", _norm_polnum("CXS 8668541"), ["Umbrella (dup check)"], queued_id))
    else:
        print("  No approved extraction found for The Glass Company.")
    print()

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n=== Summary ===")
    print(f"{'Vendor':<40} {'Norm #':<20} {'Types':<35} {'Queued Extraction'}")
    print("-" * 110)
    for row in summary:
        vendor, norm, types, ext_id = row
        print(f"{vendor:<40} {norm:<20} {str(types):<35} {ext_id}")
    print(f"\nTotal queued: {len(summary)}")


if __name__ == "__main__":
    main()
