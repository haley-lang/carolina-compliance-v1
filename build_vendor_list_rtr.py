"""
build_vendor_list_rtr.py
Phase 4: Build RTR vendor CSV files from Airtable Incoming Extractions data.
Read-only — NO Airtable writes.

Outputs:
  /Users/haleybridges/Desktop/RTR Construction LLC/vendor_list_rtr.csv
  /Users/haleybridges/Desktop/RTR Construction LLC/vendor_list_rtr_needs_decision.csv
"""

import csv
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Field ID constants (Airtable cellValuesByFieldId keys)
# ---------------------------------------------------------------------------
FLD_SOURCE_FILENAME = "fldHAwdxnX3yM0s3o"   # string
FLD_RAW_JSON        = "fldTGsebc6o2ll5Pu"   # string (parse as JSON)
FLD_REVIEW_STATUS   = "fldYNgd0wiaxzXKS9"   # singleSelect {id, name, color}
FLD_CONFIDENCE      = "fld1Y2QrenfEEOpsv"   # float
FLD_NAMED_INSURED   = "fld4X90MLBQIqTNTn"   # string
FLD_REVIEW_REASON   = "fldJ6cZfcRCT4EWz0"   # singleSelect

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_FILE = Path(
    "/Users/haleybridges/.claude/projects/-Users-haleybridges"
    "/59ba0420-e2ca-455e-8fee-01267807f0dd/tool-results"
    "/mcp-claude_ai_Airtable-list_records_for_table-1791593542403.txt"
)
OUTPUT_DIR = Path("/Users/haleybridges/Desktop/RTR Construction LLC")
OUT_MAIN   = OUTPUT_DIR / "vendor_list_rtr.csv"
OUT_NEEDS  = OUTPUT_DIR / "vendor_list_rtr_needs_decision.csv"

# cert18: contact emails flagged as unreliable scan
CERT18_FILENAME = "COI_forms_cert18.json"


# ---------------------------------------------------------------------------
# RTR filename filter
# Billy/RTR: starts with "COI_forms_", "RTR_COI_", "N9WC", "coi.json",
# or contains "resend" (case-insensitive)
# ---------------------------------------------------------------------------

def is_rtr_filename(fn: str) -> bool:
    fl = fn.lower()
    if fl.startswith("coi_forms_"):
        return True
    if fl.startswith("rtr_coi_"):
        return True
    if fl.startswith("n9wc"):
        return True
    if fl == "coi.json":
        return True
    if "resend" in fl:
        return True
    return False


# ---------------------------------------------------------------------------
# Vendor name normalization
# ---------------------------------------------------------------------------

def normalize_vendor_name(raw: str) -> str:
    """
    Normalize a vendor name:
      1. Strip leading/trailing whitespace
      2. Normalize inner spaces to single space
      3. Detect and canonicalize company suffix (LLC, Inc., etc.)
      4. Title-case the non-suffix portion
      5. Preserve dba, /, & as-is
      6. Strip trailing period from full name if not part of recognized suffix
    """
    if not raw or not raw.strip():
        return "(unknown)"

    s = raw.strip()
    s = re.sub(r"\s+", " ", s)

    # Suffix patterns applied case-insensitively, anchored to end of string.
    # Each yields (captured_prefix, canonical_suffix).
    # Order matters: PLLC before LLC, LLP before LP.
    suffix_patterns = [
        (r"^(.*?)\s*P\.?L\.?L\.?C\.?\s*$", "PLLC"),
        (r"^(.*?)\s*L\.?L\.?P\.?\s*$",     "LLP"),
        (r"^(.*?)\s*L\.?L\.?C\.?\s*$",     "LLC"),
        (r"^(.*?)\s*Inc\.?\s*$",            "Inc."),
        (r"^(.*?)\s*Corp\.?\s*$",           "Corp."),
        (r"^(.*?)\s*Ltd\.?\s*$",            "Ltd."),
        (r"^(.*?)\s*L\.?P\.?\s*$",          "LP"),
        (r"^(.*?)\s*Co\.?\s*$",             "Co."),
    ]

    suffix_found = None
    prefix = s
    for pattern, canonical in suffix_patterns:
        m = re.match(pattern, s, re.IGNORECASE)
        if m:
            prefix = m.group(1).strip()
            suffix_found = canonical
            break

    titled = _title_case_words(prefix)

    if suffix_found:
        s = (titled + " " + suffix_found).strip() if titled else suffix_found
    else:
        s = titled

    # Strip trailing period if it's not part of a recognized suffix ending
    recognized_suffix_endings = ("Inc.", "Corp.", "Ltd.", "Co.", "PLLC", "LLC", "LLP", "LP")
    if s.endswith(".") and not any(s.endswith(sfx) for sfx in recognized_suffix_endings):
        s = s[:-1].rstrip()

    return s


def _title_case_word(word: str) -> str:
    """Title-case a single word, handling dotted abbreviations and apostrophes."""
    if not word:
        return word
    # Dotted abbreviations like "E.E." or "J.K.P." -> keep uppercase
    if re.match(r'^([A-Za-z]\.)+$', word):
        return word.upper()
    if re.match(r'^([A-Za-z]\.)+[A-Za-z]$', word):
        return word.upper()
    # Apostrophe handling: "DUANE'S" -> "Duane's"
    if "'" in word:
        parts = word.split("'")
        titled_parts = []
        for j, part in enumerate(parts):
            if j == 0:
                titled_parts.append(part.capitalize())
            else:
                titled_parts.append(part.lower())
        return "'".join(titled_parts)
    return word.capitalize()


def _title_case_words(s: str) -> str:
    """Title-case a multi-word string with special-case handling."""
    if not s:
        return s
    words = s.split(" ")
    result = []
    small_words = {"a", "an", "the", "of", "and", "or", "in", "on", "at", "to", "for", "with", "by"}
    for i, word in enumerate(words):
        if not word:
            result.append(word)
            continue
        lower = word.lower()
        # Preserve tokens as written
        if lower == "dba":
            result.append("dba")
            continue
        if lower == "d/b/a":
            result.append("d/b/a")
            continue
        if lower in ("&", "/"):
            result.append(word)
            continue
        # Small connecting words stay lowercase except at position 0
        if i > 0 and lower in small_words:
            result.append(lower)
            continue
        result.append(_title_case_word(word))

    # Always capitalize the first word
    if result and result[0]:
        first = result[0]
        if first and first[0].islower():
            result[0] = first[0].upper() + first[1:]
    return " ".join(result)


# ---------------------------------------------------------------------------
# Policy type normalization
# ---------------------------------------------------------------------------

def normalize_policy_type(pt: str) -> str:
    """Map verbose policy type labels to short codes per spec."""
    if not pt:
        return ""
    pt_lower = pt.lower().strip()
    if "commercial general liability" in pt_lower or "general liability" in pt_lower:
        return "GL"
    if (
        "commercial auto" in pt_lower
        or "automobile liability" in pt_lower
        or "auto liability" in pt_lower
    ):
        return "Auto"
    if "workers compensation" in pt_lower or "workers' compensation" in pt_lower:
        return "WC"
    if "umbrella liability" in pt_lower or "umbrella liab" in pt_lower:
        return "Umbrella"
    if "excess liability" in pt_lower:
        return "Excess"
    return pt.strip()


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def parse_date(s: str):
    """Try multiple date formats. Returns datetime or None."""
    if not s or not s.strip():
        return None
    s = s.strip()
    # Normalize M/D/YYYY -> MM/DD/YYYY
    s_norm = re.sub(
        r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$",
        lambda m: f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}",
        s,
    )
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s_norm, fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Variant detection key
# ---------------------------------------------------------------------------

def ultra_normalize(s: str) -> str:
    """Remove all punctuation, lowercase, collapse spaces — for variant detection."""
    return re.sub(r"\s+", "", re.sub(r"[^a-z0-9]", "", s.lower()))


# ---------------------------------------------------------------------------
# Split two-company named insured string
# ---------------------------------------------------------------------------

def split_two_insureds(raw_ni: str) -> list:
    """
    Try to split a named_insured that contains two companies into a list of parts.
    Returns list of 1 or 2 strings.
    """
    if not raw_ni:
        return ["(unknown)"]

    # Explicit separator: newline
    if "\n" in raw_ni:
        parts = [p.strip() for p in raw_ni.split("\n") if p.strip()]
        if len(parts) >= 2:
            return parts[:2]

    # Explicit separator: " and " (case-insensitive)
    m = re.search(r"\s+and\s+", raw_ni, re.IGNORECASE)
    if m:
        parts = [raw_ni[:m.start()].strip(), raw_ni[m.end():].strip()]
        if all(parts):
            return parts

    # Explicit separator: " & "
    m = re.search(r"\s+&\s+", raw_ni)
    if m:
        parts = [raw_ni[:m.start()].strip(), raw_ni[m.end():].strip()]
        if all(parts):
            return parts

    # Implicit: split on suffix boundary followed by capital letter
    # Pattern: match first occurrence of a company suffix followed by space+capital
    # Use re.split with a capturing group to identify the boundary
    pattern = r"(Inc\.|LLC|Corp\.|Ltd\.|PLLC|LLP)\s+(?=[A-Z])"
    split_parts = re.split(pattern, raw_ni, maxsplit=1)
    # split_parts = [before_suffix, suffix, after_boundary]
    if len(split_parts) == 3:
        part1 = (split_parts[0] + split_parts[1]).strip()
        part2 = split_parts[2].strip()
        if part1 and part2:
            return [part1, part2]

    # No split found — return as single item
    return [raw_ni.strip()]


# ---------------------------------------------------------------------------
# Load Airtable data from cached file
# ---------------------------------------------------------------------------

def load_records():
    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("records", [])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_files():
    records = load_records()
    print(f"Total records loaded: {len(records)}")

    # ------------------------------------------------------------------
    # Step 1: Filter records
    # ------------------------------------------------------------------
    rtr_rows = []
    skipped_rejected = 0
    skipped_not_rtr = 0

    for rec in records:
        fields = rec.get("cellValuesByFieldId", {})

        # Review Status
        rs_raw = fields.get(FLD_REVIEW_STATUS, {})
        if isinstance(rs_raw, dict):
            review_status = rs_raw.get("name", "")
        else:
            review_status = rs_raw or ""

        # Skip Rejected
        if review_status == "Rejected":
            skipped_rejected += 1
            continue

        # Source filename
        src_filename = fields.get(FLD_SOURCE_FILENAME, "") or ""

        # RTR filter
        if not is_rtr_filename(src_filename):
            skipped_not_rtr += 1
            continue

        # Parse Raw JSON
        raw_json_str = fields.get(FLD_RAW_JSON, "") or ""
        try:
            parsed = json.loads(raw_json_str) if raw_json_str.strip() else {}
        except (json.JSONDecodeError, TypeError):
            parsed = {}

        # Named insured: Airtable field is primary source
        named_insured_at = (fields.get(FLD_NAMED_INSURED, "") or "").strip()
        # Raw JSON named_insured: used for name_as_printed
        named_insured_json = str(parsed.get("named_insured") or "").strip()

        # Use Airtable field first, fall back to JSON
        named_insured = named_insured_at if named_insured_at else named_insured_json

        rtr_rows.append({
            "src_filename": src_filename,
            "review_status": review_status,
            "named_insured": named_insured,
            "raw_named_insured": named_insured_json,
            "parsed": parsed,
        })

    print(f"Skipped (Rejected): {skipped_rejected}")
    print(f"Skipped (not RTR): {skipped_not_rtr}")
    print(f"RTR rows to process: {len(rtr_rows)}")

    # ------------------------------------------------------------------
    # Step 2: Aggregate by normalized vendor name
    # ------------------------------------------------------------------
    vendors: dict[str, dict] = {}

    for row in rtr_rows:
        src_filename    = row["src_filename"]
        review_status   = row["review_status"]
        named_insured   = row["named_insured"] or "(unknown)"
        raw_named_insured = row["raw_named_insured"] or named_insured
        parsed          = row["parsed"]

        norm = normalize_vendor_name(named_insured)

        if norm not in vendors:
            vendors[norm] = {
                "vendor_name": norm,
                "raw_names": set(),
                "num_certificates": 0,
                "source_filenames": [],
                "trades_or_notes": [],
                "contact_emails": set(),
                "policy_types": set(),
                "expiration_dates": [],   # list of (datetime_or_None, raw_str)
                "statuses": set(),
            }

        v = vendors[norm]
        v["num_certificates"] += 1
        v["raw_names"].add(raw_named_insured)
        v["source_filenames"].append(src_filename)
        v["statuses"].add(review_status)

        # trades_or_notes: description_of_operations
        desc = parsed.get("description_of_operations")
        if desc and str(desc).strip() and str(desc).strip().lower() not in ("none", "null", ""):
            trimmed = str(desc).strip()[:200]
            if trimmed not in v["trades_or_notes"]:
                v["trades_or_notes"].append(trimmed)

        # contact_emails
        emails_raw = parsed.get("contact_emails") or []
        if isinstance(emails_raw, list):
            for email in emails_raw:
                if not email:
                    continue
                email_str = str(email).strip()
                if not email_str:
                    continue
                if src_filename == CERT18_FILENAME:
                    v["contact_emails"].add(f"unreliable scan: {email_str}")
                else:
                    v["contact_emails"].add(email_str)

        # policy_types and expiration dates
        policies = parsed.get("policies") or []
        if isinstance(policies, list):
            for policy in policies:
                if not isinstance(policy, dict):
                    continue
                pt = policy.get("policy_type") or ""
                norm_pt = normalize_policy_type(pt)
                if norm_pt:
                    v["policy_types"].add(norm_pt)

                exp_raw = str(policy.get("expiration_date") or "").strip()
                if exp_raw and exp_raw.lower() not in ("none", "null", ""):
                    dt = parse_date(exp_raw)
                    v["expiration_dates"].append((dt, exp_raw))

    print(f"Unique normalized vendors: {len(vendors)}")

    # ------------------------------------------------------------------
    # Step 3: Build File 1 rows
    # ------------------------------------------------------------------
    # Policy type sort order
    PT_ORDER = {"GL": 0, "Auto": 1, "WC": 2, "Umbrella": 3, "Excess": 4}

    file1_rows = []
    for norm in sorted(vendors.keys(), key=lambda x: x.lower()):
        v = vendors[norm]

        # name_as_printed: unique raw names sorted, joined by " / "
        name_as_printed = " / ".join(sorted(v["raw_names"]))

        # source_filenames: deduplicated, sorted, joined by " | "
        seen_fns = []
        seen_fn_set: set = set()
        for fn in sorted(v["source_filenames"]):
            if fn not in seen_fn_set:
                seen_fns.append(fn)
                seen_fn_set.add(fn)
        source_filenames = " | ".join(seen_fns)

        # trades_or_notes
        trades = " | ".join(v["trades_or_notes"]) if v["trades_or_notes"] else ""

        # contact_emails
        contact_emails = " | ".join(sorted(v["contact_emails"])) if v["contact_emails"] else ""

        # policy_types sorted
        pts_sorted = sorted(
            v["policy_types"],
            key=lambda x: (PT_ORDER.get(x, 99), x),
        )
        policy_types = " | ".join(pts_sorted) if pts_sorted else ""

        # earliest_expiration
        if v["expiration_dates"]:
            parsed_dates = [(dt, raw) for dt, raw in v["expiration_dates"] if dt is not None]
            unparsed    = [(dt, raw) for dt, raw in v["expiration_dates"] if dt is None]
            if parsed_dates:
                min_dt, _ = min(parsed_dates, key=lambda x: x[0])
                earliest_expiration = min_dt.strftime("%Y-%m-%d")
            elif unparsed:
                earliest_expiration = unparsed[0][1]
            else:
                earliest_expiration = ""
        else:
            earliest_expiration = ""

        # status
        status = " / ".join(sorted(v["statuses"]))

        file1_rows.append({
            "vendor_name":         v["vendor_name"],
            "name_as_printed":     name_as_printed,
            "num_certificates":    v["num_certificates"],
            "source_filenames":    source_filenames,
            "trades_or_notes":     trades,
            "contact_emails":      contact_emails,
            "policy_types":        policy_types,
            "earliest_expiration": earliest_expiration,
            "status":              status,
        })

    # ------------------------------------------------------------------
    # Step 4: Build File 2 — Needs Decision
    # ------------------------------------------------------------------
    file2_rows = []

    # KIND A: Spelling variants
    # Group vendor names that share the same ultra-normalized key
    ultra_map: dict[str, list] = defaultdict(list)
    for norm in vendors:
        ultra_map[ultra_normalize(norm)].append(norm)

    variant_groups = {k: v for k, v in ultra_map.items() if len(v) >= 2}

    for key in sorted(variant_groups):
        norm_list = sorted(variant_groups[key])
        for norm in norm_list:
            v = vendors[norm]
            others = [n for n in norm_list if n != norm]
            notes = "possible spelling variant of: " + " / ".join(others)

            seen_fns_a: list = []
            seen_fn_set_a: set = set()
            for fn in sorted(v["source_filenames"]):
                if fn not in seen_fn_set_a:
                    seen_fns_a.append(fn)
                    seen_fn_set_a.add(fn)

            file2_rows.append({
                "candidate_vendor_name": norm,
                "raw_names":             " / ".join(sorted(v["raw_names"])),
                "num_certs":             v["num_certificates"],
                "source_filenames":      " | ".join(seen_fns_a),
                "notes":                 notes,
            })

    # KIND B: Two insureds on cert33 and cert34 — always output per spec
    cert_b_targets = sorted(["COI_forms_cert33.json", "COI_forms_cert34.json"])

    for target_fn in cert_b_targets:
        matching = [r for r in rtr_rows if r["src_filename"] == target_fn]
        if not matching:
            # No matching row found — still output a placeholder row
            file2_rows.append({
                "candidate_vendor_name": "(not found)",
                "raw_names":             "",
                "num_certs":             0,
                "source_filenames":      target_fn,
                "notes":                 "two insureds on one certificate, Haley decides",
            })
            continue

        row = matching[0]
        raw_ni = row["raw_named_insured"] or row["named_insured"] or ""

        parts = split_two_insureds(raw_ni)

        if len(parts) >= 2:
            for part in parts:
                file2_rows.append({
                    "candidate_vendor_name": normalize_vendor_name(part),
                    "raw_names":             raw_ni,
                    "num_certs":             1,
                    "source_filenames":      target_fn,
                    "notes":                 "two insureds on one certificate, Haley decides",
                })
        else:
            # Single name — still output one row per spec
            file2_rows.append({
                "candidate_vendor_name": normalize_vendor_name(parts[0]) if parts else "(unknown)",
                "raw_names":             raw_ni,
                "num_certs":             1,
                "source_filenames":      target_fn,
                "notes":                 "two insureds on one certificate, Haley decides",
            })

    # ------------------------------------------------------------------
    # Step 5: Write File 1
    # ------------------------------------------------------------------
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    f1_cols = [
        "vendor_name", "name_as_printed", "num_certificates",
        "source_filenames", "trades_or_notes", "contact_emails",
        "policy_types", "earliest_expiration", "status",
    ]
    with open(OUT_MAIN, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=f1_cols)
        writer.writeheader()
        writer.writerows(file1_rows)

    print(f"\nFile 1 written: {OUT_MAIN}")
    print(f"  Vendors: {len(file1_rows)}")

    # ------------------------------------------------------------------
    # Step 6: Write File 2
    # ------------------------------------------------------------------
    f2_cols = [
        "candidate_vendor_name", "raw_names", "num_certs",
        "source_filenames", "notes",
    ]
    with open(OUT_NEEDS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=f2_cols)
        writer.writeheader()
        writer.writerows(file2_rows)

    print(f"File 2 written: {OUT_NEEDS}")
    print(f"  Rows: {len(file2_rows)}")

    # ------------------------------------------------------------------
    # Data quality diagnostics
    # ------------------------------------------------------------------
    print("\n--- Data Quality Notes ---")
    missing_ni = [r["src_filename"] for r in rtr_rows
                  if not r["named_insured"] or r["named_insured"] == "(unknown)"]
    if missing_ni:
        # Deduplicate (N9WC appears twice)
        uniq = sorted(set(missing_ni))
        print(f"Certs with no named insured (mapped to '(unknown)'): {uniq}")

    no_exp = [norm for norm, v in vendors.items() if not v["expiration_dates"]]
    if no_exp:
        print(f"Vendors with no expiration dates: {no_exp}")

    no_email = [norm for norm, v in vendors.items() if not v["contact_emails"]]
    print(f"Vendors with no contact emails: {len(no_email)} vendors")

    if variant_groups:
        print(f"Spelling variant groups (Kind A): {len(variant_groups)}")
    else:
        print("Spelling variant groups (Kind A): none detected")

    return len(file1_rows), len(file2_rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    v_count, r_count = build_files()
    print(f"\n=== SUMMARY ===")
    print(f"Vendors in vendor_list_rtr.csv:              {v_count}")
    print(f"Rows in vendor_list_rtr_needs_decision.csv:  {r_count}")
    print(f"\nOutput files:")
    print(f"  {OUT_MAIN}")
    print(f"  {OUT_NEEDS}")
