"""
build_vendor_list_rtr.py
Reads RTR Incoming Extractions rows from Airtable and produces a CSV report
at /Users/haleybridges/Desktop/RTR Construction LLC/vendor_list_rtr.csv
"""

import csv
import json
import os
import re
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config
import eval_scoring
from airtable_importer import clean_base_id
from pyairtable import Api

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_ID = "appCGgww0Pt7KE04u"
OUTPUT_PATH = Path("/Users/haleybridges/Desktop/RTR Construction LLC/vendor_list_rtr.csv")

RTR_FILENAME_PREFIXES = (
    "coi_forms_cert",
    "coi_forms1_cert",
    "rtr_coi_",
)
RTR_EXACT_FILENAMES = {"n9wc394833_acordapp25_i"}

POLICY_FAMILIES = ["GL", "AUTO", "WC", "UMBRELLA", "EXCESS", "EXCESS_WC", "OTHER"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_filename(s: str) -> str:
    """Lowercase + replace non-alnum with underscore."""
    return re.sub(r"[^a-z0-9]", "_", (s or "").lower())


def is_rtr_filename(raw_filename: str) -> bool:
    norm = normalize_filename(raw_filename)
    for prefix in RTR_FILENAME_PREFIXES:
        if norm.startswith(prefix):
            return True
    return norm in RTR_EXACT_FILENAMES


def is_rtr_cert_holder(parsed_json: dict) -> bool:
    holder = str(parsed_json.get("certificate_holder") or "")
    return "rtr" in holder.lower()


def normalize_vendor_name(raw: str) -> str:
    """Apply the 6-step normalization described in the spec."""
    s = (raw or "").strip()
    # Step 2: replace commas and periods with space
    s = re.sub(r"[,.]", " ", s)
    # Step 3: collapse multiple whitespace
    s = re.sub(r"\s+", " ", s)
    # Step 4: lowercase
    s = s.lower()
    # Step 5: normalize suffix variants at end of string
    s = re.sub(r"l\.?l\.?c\.?\s*$", "llc", s)
    s = re.sub(r"i\.?n\.?c\.?\s*$", "inc", s)
    s = re.sub(r"\bco\.\s*$", "co", s)
    s = re.sub(r"corp\.\s*$", "corp", s)
    s = re.sub(r"\bltd\.\s*$", "ltd", s)
    # Step 6: strip again
    s = s.strip()
    return s


def split_multi_insured(named_insured: str) -> list[str]:
    """Split on ' and ', ' & ', ' / ' (case-insensitive). Returns list of parts."""
    parts = re.split(r"(?i)\s+and\s+|\s+&\s+|\s+/\s+", named_insured)
    return [p.strip() for p in parts if p.strip()]


def suffix_strip(normalized: str) -> str:
    """Strip trailing business suffix for duplicate detection."""
    return re.sub(r"\s+(llc|inc|co|corp|ltd|pllc|pc)\s*$", "", normalized).strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    api_key = config.AIRTABLE_API_KEY
    base_id = clean_base_id(BASE_ID)

    print("Fetching all rows from Incoming Extractions…", flush=True)
    api = Api(api_key)
    table = api.table(base_id, "Incoming Extractions")
    rows = table.all()
    print(f"  Total rows fetched: {len(rows)}", flush=True)

    # ------------------------------------------------------------------
    # Filter to RTR rows
    # ------------------------------------------------------------------
    rtr_rows = []
    skipped_rejected = 0
    skipped_test = 0
    skipped_no_json = 0

    for row in rows:
        fields = row.get("fields", {})

        # Exclusion: Rejected
        if fields.get("Review Status") == "Rejected":
            skipped_rejected += 1
            continue

        # Exclusion: test/scenario filenames
        src_filename = fields.get("Source Filename", "")
        norm_fn = normalize_filename(src_filename)
        if norm_fn.startswith("scenario_") or norm_fn.startswith("test_"):
            skipped_test += 1
            continue

        # Exclusion: missing or unparseable Raw JSON
        raw_json_str = fields.get("Raw JSON")
        if not raw_json_str:
            skipped_no_json += 1
            continue
        try:
            parsed = json.loads(raw_json_str)
        except (json.JSONDecodeError, TypeError):
            skipped_no_json += 1
            continue

        # RTR filter: filename pattern OR cert holder contains RTR
        if not (is_rtr_filename(src_filename) or is_rtr_cert_holder(parsed)):
            continue

        rtr_rows.append((fields, parsed))

    print(f"  RTR rows after filtering: {len(rtr_rows)}")
    print(f"  Skipped — Rejected: {skipped_rejected}, test/scenario: {skipped_test}, no valid JSON: {skipped_no_json}")

    # ------------------------------------------------------------------
    # Build vendor data structures
    # ------------------------------------------------------------------
    # Key: normalized_vendor_name
    # Value: dict with all aggregated data
    vendors: dict[str, dict] = {}

    non_coi_rows = []  # For section 3

    for fields, parsed in rtr_rows:
        src_filename = fields.get("Source Filename", "")
        raw_named_insured = str(parsed.get("named_insured") or "").strip()
        document_type = str(parsed.get("document_type") or "").strip()
        review_status = fields.get("Review Status", "")
        ai_disagreements = fields.get("AI Disagreements", "")

        # Confidence — try numeric field first, fall back to JSON
        confidence_raw = fields.get("Confidence") or parsed.get("confidence")
        try:
            confidence = float(confidence_raw) if confidence_raw is not None else 1.0
        except (ValueError, TypeError):
            confidence = 1.0

        # Track non-COI for section 3
        if document_type.upper() != "COI":
            non_coi_rows.append({
                "source_filename": src_filename,
                "document_type": document_type,
                "named_insured": raw_named_insured,
                "review_status": review_status,
            })

        # Detect multi-company named insured
        multi = bool(re.search(r"(?i)\s+and\s+|\s+&\s+|\s+/\s+", raw_named_insured))
        if multi:
            candidate_parts = split_multi_insured(raw_named_insured)
        else:
            candidate_parts = [raw_named_insured] if raw_named_insured else []

        if not candidate_parts:
            candidate_parts = ["(unknown)"]

        # Process each candidate vendor name from this certificate
        for raw_part in candidate_parts:
            norm = normalize_vendor_name(raw_part)
            if not norm:
                norm = "(unknown)"

            if norm not in vendors:
                vendors[norm] = {
                    "normalized_vendor_name": norm,
                    "name_variants": set(),
                    "num_certificates": 0,
                    "source_filenames": set(),
                    # Policy family tracking: family -> (latest_exp_iso, policy_number)
                    "policies": {fam: None for fam in POLICY_FAMILIES},
                    # Flags
                    "multi_company_insured": False,
                    "non_coi_cert": False,
                    "low_confidence": False,
                    "missing_policy_fields": False,
                    "ai_disagreed": False,
                }

            v = vendors[norm]
            v["name_variants"].add(raw_named_insured)
            v["source_filenames"].add(src_filename)
            v["num_certificates"] += 1

            # Flags
            if multi:
                v["multi_company_insured"] = True
            if document_type.upper() != "COI":
                v["non_coi_cert"] = True
            if confidence < 0.85:
                v["low_confidence"] = True
            if ai_disagreements and str(ai_disagreements).strip():
                v["ai_disagreed"] = True

            # Policies
            policies = parsed.get("policies") or []
            if not isinstance(policies, list):
                policies = []
            for policy in policies:
                if not isinstance(policy, dict):
                    continue

                policy_number = str(policy.get("policy_number") or "").strip()
                eff_date = str(policy.get("effective_date") or "").strip()
                exp_date = str(policy.get("expiration_date") or "").strip()

                # missing_policy_fields flag
                if not policy_number or (not eff_date and not exp_date):
                    v["missing_policy_fields"] = True

                fam = eval_scoring.family(policy.get("policy_type"))

                # Track latest expiration per family
                if exp_date:
                    current = v["policies"][fam]
                    if current is None or exp_date > current[0]:
                        v["policies"][fam] = (exp_date, policy_number)

    # ------------------------------------------------------------------
    # Deduplicate: non-COI rows by source_filename to avoid repeats
    # ------------------------------------------------------------------
    seen_non_coi = set()
    deduped_non_coi = []
    for r in non_coi_rows:
        key = (r["source_filename"], r["document_type"])
        if key not in seen_non_coi:
            seen_non_coi.add(key)
            deduped_non_coi.append(r)

    # ------------------------------------------------------------------
    # Section 2: Possible duplicates
    # ------------------------------------------------------------------
    vendor_names = sorted(vendors.keys())
    stripped_map: dict[str, list[str]] = defaultdict(list)
    for name in vendor_names:
        stripped = suffix_strip(name)
        if stripped:  # only consider non-empty stripped forms
            stripped_map[stripped].append(name)

    duplicate_pairs = []
    for stripped, names in stripped_map.items():
        if len(names) >= 2:
            for a, b in combinations(sorted(names), 2):
                if a != b:
                    duplicate_pairs.append({
                        "name_a": a,
                        "name_b": b,
                        "reason": f"both strip to '{stripped}' after removing business suffix",
                    })

    # ------------------------------------------------------------------
    # Build CSV rows for section 1
    # ------------------------------------------------------------------
    vendor_rows = []
    for norm in sorted(vendors.keys()):
        v = vendors[norm]
        flags = []
        if v["multi_company_insured"]:
            flags.append("multi_company_insured")
        if v["non_coi_cert"]:
            flags.append("non_coi_cert")
        if v["low_confidence"]:
            flags.append("low_confidence")
        if v["missing_policy_fields"]:
            flags.append("missing_policy_fields")
        if v["ai_disagreed"]:
            flags.append("ai_disagreed")

        def policy_cols(fam):
            entry = v["policies"].get(fam)
            if entry:
                return entry[0], entry[1]
            return "", ""

        gl_exp, gl_pol = policy_cols("GL")
        auto_exp, auto_pol = policy_cols("AUTO")
        wc_exp, wc_pol = policy_cols("WC")
        umb_exp, umb_pol = policy_cols("UMBRELLA")
        exc_exp, exc_pol = policy_cols("EXCESS")

        vendor_rows.append({
            "normalized_vendor_name": norm,
            "name_variants": "; ".join(sorted(v["name_variants"])),
            "num_certificates": v["num_certificates"],
            "source_filenames": "; ".join(sorted(v["source_filenames"])),
            "gl_latest_exp": gl_exp,
            "gl_policy_number": gl_pol,
            "auto_latest_exp": auto_exp,
            "auto_policy_number": auto_pol,
            "wc_latest_exp": wc_exp,
            "wc_policy_number": wc_pol,
            "umbrella_latest_exp": umb_exp,
            "umbrella_policy_number": umb_pol,
            "excess_latest_exp": exc_exp,
            "excess_policy_number": exc_pol,
            "flags": "; ".join(flags),
        })

    # ------------------------------------------------------------------
    # Write CSV
    # ------------------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Section 1: Vendor List
        writer.writerow(["VENDOR LIST"])
        writer.writerow([
            "normalized_vendor_name", "name_variants", "num_certificates", "source_filenames",
            "gl_latest_exp", "gl_policy_number",
            "auto_latest_exp", "auto_policy_number",
            "wc_latest_exp", "wc_policy_number",
            "umbrella_latest_exp", "umbrella_policy_number",
            "excess_latest_exp", "excess_policy_number",
            "flags",
        ])
        for r in vendor_rows:
            writer.writerow([
                r["normalized_vendor_name"],
                r["name_variants"],
                r["num_certificates"],
                r["source_filenames"],
                r["gl_latest_exp"],
                r["gl_policy_number"],
                r["auto_latest_exp"],
                r["auto_policy_number"],
                r["wc_latest_exp"],
                r["wc_policy_number"],
                r["umbrella_latest_exp"],
                r["umbrella_policy_number"],
                r["excess_latest_exp"],
                r["excess_policy_number"],
                r["flags"],
            ])

        # Blank row separator
        writer.writerow([])

        # Section 2: Possible Duplicates
        writer.writerow(["POSSIBLE DUPLICATES — NEEDS HALEY'S DECISION"])
        writer.writerow(["name_a", "name_b", "reason"])
        for d in duplicate_pairs:
            writer.writerow([d["name_a"], d["name_b"], d["reason"]])

        # Blank row separator
        writer.writerow([])

        # Section 3: Non-COI Documents
        writer.writerow(["NON-COI DOCUMENTS"])
        writer.writerow(["source_filename", "document_type", "named_insured", "review_status"])
        for r in deduped_non_coi:
            writer.writerow([
                r["source_filename"],
                r["document_type"],
                r["named_insured"],
                r["review_status"],
            ])

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n--- Summary ---")
    print(f"Total RTR rows found:         {len(rtr_rows)}")
    print(f"Vendor count (normalized):    {len(vendors)}")
    print(f"Possible duplicate pairs:     {len(duplicate_pairs)}")
    print(f"Non-COI document rows:        {len(deduped_non_coi)}")
    print(f"\nCSV written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
