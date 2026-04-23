import os
import logging
from pyairtable import Api
from datetime import date, datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Airtable configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Table names
TABLE_VENDORS = "Vendors"
TABLE_CLIENT_REQUIREMENTS = "Client Requirements"
TABLE_INSURANCE_POLICIES = "Insurance Policies"

# Compliance status values
STATUS_COMPLIANT = "Compliant"
STATUS_MISSING_COVERAGE = "Missing Coverage"
STATUS_EXPIRED = "Expired"
STATUS_NEEDS_REVIEW = "Needs Review"

def fetch_records(table):
    """Fetch all records from a given table."""
    try:
        records = table.all()
        logger.info("Loaded %d records from table", len(records))
        return records
    except Exception as e:
        logger.error("Failed to fetch records: %s", e)
        return []

def fetch_active_assignments_for_vendor(vendor_id, assignments_table):
    """Fetch active assignments for a specific vendor."""
    try:
        records = assignments_table.all()
        filtered_records = [
            record for record in records
            if record["fields"].get("Vendor Link", [None])[0] == vendor_id and record["fields"].get("Active") == True
        ]
        logger.info("Loaded %d active assignments for vendor %s", len(filtered_records), vendor_id)
        return filtered_records
    except Exception as e:
        logger.error("Failed to fetch active assignments: %s", e)
        return []

def fetch_policies_for_vendor(vendor_id, insurance_policies_table):
    """Fetch insurance policies for a specific vendor."""
    try:
        records = insurance_policies_table.all()
        filtered_records = [
            record for record in records
            if record["fields"].get("Vendor Link", [None])[0] == vendor_id
        ]
        logger.info("Loaded %d policies for vendor %s", len(filtered_records), vendor_id)
        return filtered_records
    except Exception as e:
        logger.error("Failed to fetch policies: %s", e)
        return []

def fetch_requirements_for_client(client_id, client_requirements_table):
    """Fetch client requirements for a specific client."""
    try:
        records = client_requirements_table.all()
        filtered_records = [
            record for record in records
            if record["fields"].get("Client Link", [None])[0] == client_id
        ]
        logger.info("Loaded %d requirements for client %s", len(filtered_records), client_id)
        return filtered_records
    except Exception as e:
        logger.error("Failed to fetch requirements: %s", e)
        return []

def validate_vendor(vendor, client_requirements, insurance_policies):
    """Validate a vendor's compliance status."""
    vendor_id = vendor["id"]
    vendor_name = vendor["fields"].get("Vendor Name", vendor_id)
    logger.info("Validating vendor: %s", vendor_name)

    # Find linked client requirements
    client_id = vendor["fields"].get("Client Link", [None])[0]
    if not client_id:
        logger.warning("Vendor %s has no linked client", vendor_name)
        return STATUS_NEEDS_REVIEW

    # Filter client requirements for this client
    client_reqs = [req for req in client_requirements if req["fields"].get("Client Link", [None])[0] == client_id]

    # Check if client requirements are empty
    if len(client_reqs) == 0:
        logger.warning("Vendor %s linked client has no requirements", vendor_name)
        return STATUS_NEEDS_REVIEW

    # Filter insurance policies for this vendor
    vendor_policies = [policy for policy in insurance_policies if policy["fields"].get("Vendor Link", [None])[0] == vendor_id]

    compliance_status = STATUS_COMPLIANT

    # Validate each requirement
    for req in client_reqs:
        policy_type = req["fields"].get("Policy Type")
        required = req["fields"].get("Required", False)

        # Check for required policy
        matching_policies = [p for p in vendor_policies if p["fields"].get("Policy Type") == policy_type]
        if required and not matching_policies:
            logger.info("Missing required policy: %s", policy_type)
            return STATUS_MISSING_COVERAGE

        # Check for expired policy
        for policy in matching_policies:
            expiry_date = policy["fields"].get("Expiration Date")
            if expiry_date and date.fromisoformat(expiry_date) < date.today():
                logger.info("Expired policy: %s", policy_type)
                return STATUS_EXPIRED

        # Check for endorsement requirements
        for field in ["Additional Insured", "Waiver", "Primary Noncontributory"]:
            if req["fields"].get(f"{field} Required", False) and not any(p["fields"].get(field) for p in matching_policies):
                logger.info("Missing endorsement: %s", field)
                return STATUS_NEEDS_REVIEW

    return STATUS_COMPLIANT

def update_vendor_status(vendors_table, vendor_id, status):
    """Update the compliance status of a vendor."""
    try:
        vendors_table.update(vendor_id, {"Compliance Status": status})
        logger.info("Updated vendor %s to status: %s", vendor_id, status)
    except Exception as e:
        logger.error("Failed to update vendor status: %s", e)

# Expiration Status field on Vendors table
FLD_VENDOR_EXPIRATION_STATUS = "fldK61HV4dwDS5Kkb"
# Expiration Status field on Insurance Policies table
FLD_POLICY_EXPIRATION_STATUS = "fldXLb8RLrLc56bBx"

# ── Vendor-level policy detail field IDs (Vendors table: tblsOphSd5DKSZEro) ──
FLD_V_GL_STATUS             = "fldC7Mc729w9g64g2"
FLD_V_GL_EXPIRATION         = "fldPlnXE0LAjVBlFU"
FLD_V_WC_STATUS             = "fldH2NPUw4Kbv9HqU"
FLD_V_WC_EXPIRATION         = "fldSVqTYcisc6OO4b"
FLD_V_AUTO_STATUS           = "fldwKQHVNoSM8Qyua"
FLD_V_AUTO_EXPIRATION       = "fldKP8OOJFXHadDbB"
FLD_V_AI_ON_FILE            = "fldqHepEblGRMrdBF"
FLD_V_WOS_ON_FILE           = "fldaXCaDuDTnrgBUx"
FLD_V_NEXT_EXPIRATION       = "fldr1OkARQAaJkc5j"

# ── Insurance Policies table endorsement field IDs ──
FLD_P_AI_ON_FILE            = "flduWmW1efOyEf886"
FLD_P_WOS_ON_FILE           = "fld2neC2QvLJ16kr7"
FLD_P_POLICY_TYPE           = "fldq25Ttay2ZDC1lA"

def compute_vendor_expiration_status(vendor_policies):
    """Derive a rollup Expiration Status from all policies for a vendor."""
    statuses = set()
    for p in vendor_policies:
        status = p["fields"].get("Expiration Status") or p["fields"].get(FLD_POLICY_EXPIRATION_STATUS) or ""
        if status:
            statuses.add(status)
    if "Expired" in statuses:
        return "Expired"
    if "Expiring Soon" in statuses:
        return "Expiring Soon"
    if "Missing Expiration Date" in statuses:
        return "Missing Expiration Date"
    return "Active"

def update_vendor_expiration_status(vendors_table, vendor_id, vendor_name, vendor_policies):
    """Write the rolled-up Expiration Status to the Vendors table."""
    exp_status = compute_vendor_expiration_status(vendor_policies)
    try:
        vendors_table.update(vendor_id, {FLD_VENDOR_EXPIRATION_STATUS: exp_status})
        logger.info("Vendor %s Expiration Status → %s", vendor_name, exp_status)
    except Exception as e:
        logger.error("Failed to update Expiration Status for vendor %s: %s", vendor_name, e)

# Policy Type name → (status field, expiration field) on Vendors table
_POLICY_TYPE_VENDOR_FIELDS = {
    "General Liability": (FLD_V_GL_STATUS, FLD_V_GL_EXPIRATION),
    "Workers Comp":      (FLD_V_WC_STATUS, FLD_V_WC_EXPIRATION),
    "Auto Liability":    (FLD_V_AUTO_STATUS, FLD_V_AUTO_EXPIRATION),
}

def update_vendor_policy_detail(vendors_table, vendor_id, vendor_name, vendor_policies):
    """Write per-policy-type status, AI/WOS flags, and next expiration to the Vendors table."""
    by_type = {}
    for p in vendor_policies:
        pt = p["fields"].get("Policy Type") or p["fields"].get(FLD_P_POLICY_TYPE) or ""
        if pt not in by_type:
            by_type[pt] = []
        by_type[pt].append(p)

    fields = {}

    for type_name, (status_fld, exp_fld) in _POLICY_TYPE_VENDOR_FIELDS.items():
        policies_of_type = by_type.get(type_name, [])
        if not policies_of_type:
            fields[status_fld] = "Missing"
            fields[exp_fld] = None
            continue
        policies_of_type.sort(
            key=lambda p: p["fields"].get("Expiration Date") or "",
            reverse=True,
        )
        best = policies_of_type[0]
        raw_status = best["fields"].get("Status") or best["fields"].get("Expiration Status") or "Unknown"
        fields[status_fld] = "Active" if raw_status == "Current" else raw_status
        fields[exp_fld] = best["fields"].get("Expiration Date") or None

    all_expirations = []
    for p in vendor_policies:
        exp = p["fields"].get("Expiration Date")
        if exp:
            all_expirations.append(exp)
    if all_expirations:
        all_expirations.sort()
        fields[FLD_V_NEXT_EXPIRATION] = all_expirations[0]
    else:
        fields[FLD_V_NEXT_EXPIRATION] = None

    fields[FLD_V_AI_ON_FILE] = any(
        p["fields"].get("Additional Insured") or p["fields"].get(FLD_P_AI_ON_FILE)
        for p in vendor_policies
    )
    fields[FLD_V_WOS_ON_FILE] = any(
        p["fields"].get("Waiver") or p["fields"].get(FLD_P_WOS_ON_FILE)
        for p in vendor_policies
    )

    try:
        vendors_table.update(vendor_id, fields)
        logger.info("Vendor %s policy detail written — GL:%s WC:%s Auto:%s AI:%s WOS:%s NextExp:%s",
                     vendor_name,
                     fields.get(FLD_V_GL_STATUS), fields.get(FLD_V_WC_STATUS),
                     fields.get(FLD_V_AUTO_STATUS), fields.get(FLD_V_AI_ON_FILE),
                     fields.get(FLD_V_WOS_ON_FILE), fields.get(FLD_V_NEXT_EXPIRATION))
    except Exception as e:
        logger.error("Failed to write policy detail for vendor %s: %s", vendor_name, e)

# ── Reason codes ─────────────────────────────────────────────────────────────
RC_POLICY_MISSING                    = "POLICY_MISSING"
RC_POLICY_EXPIRED                    = "POLICY_EXPIRED"
RC_POLICY_IN_GRACE_PERIOD            = "POLICY_IN_GRACE_PERIOD"
RC_LIMIT_BELOW_MIN_PER_OCCURRENCE    = "LIMIT_BELOW_MINIMUM_PER_OCCURRENCE"
RC_LIMIT_BELOW_MIN_AGGREGATE         = "LIMIT_BELOW_MINIMUM_AGGREGATE"
RC_LIMIT_PARSE_FAILED                = "LIMIT_PARSE_FAILED"
RC_AI_NOT_EVIDENCED                  = "AI_NOT_EVIDENCED"
RC_WOS_NOT_EVIDENCED                 = "WOS_NOT_EVIDENCED"
RC_PNC_NOT_EVIDENCED                 = "PNC_NOT_EVIDENCED"
RC_COVERAGE_GAP_DETECTED             = "COVERAGE_GAP_DETECTED"

# Status priority (higher number = worse)
_STATUS_PRIORITY = {
    STATUS_COMPLIANT: 0,
    STATUS_NEEDS_REVIEW: 1,
    STATUS_EXPIRED: 2,
    STATUS_MISSING_COVERAGE: 3,
}

DEFAULT_GRACE_PERIOD_DAYS = 30


# ── Limit parsing ────────────────────────────────────────────────────────────

import re as _re

def parse_limit(limit_str):
    """Parse a requirement minimum limit value to float."""
    if not limit_str:
        return 0.0
    try:
        return float(str(limit_str).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _parse_dollar_amount(text: str) -> float:
    """Parse a single dollar amount from text. Handles $1,000,000 and 1000000 and 1M."""
    text = text.strip().replace("$", "").replace(",", "").replace(" ", "")
    # Handle shorthand: 1M, 2M, 500K
    m = _re.match(r"^([\d.]+)\s*[Mm]$", text)
    if m:
        return float(m.group(1)) * 1_000_000
    m = _re.match(r"^([\d.]+)\s*[Kk]$", text)
    if m:
        return float(m.group(1)) * 1_000
    try:
        return float(text)
    except (ValueError, TypeError):
        return 0.0


def extract_limits_from_policy(policy) -> dict:
    """Extract per_occurrence and aggregate limits from Coverage Limits text.

    Returns {"per_occurrence": float, "aggregate": float, "parse_ok": bool}
    """
    coverage_text = (policy["fields"].get("Coverage Limits", "") or "").strip()
    if not coverage_text:
        return {"per_occurrence": 0.0, "aggregate": 0.0, "parse_ok": False}

    per_occurrence = 0.0
    aggregate = 0.0
    found_any = False

    # Pattern 1: labeled values — "EACH OCCURRENCE $1,000,000"
    occ_match = _re.search(
        r"(?:EACH OCCURRENCE|PER OCCURRENCE|EA\s*OCC(?:URRENCE)?|CSL)\s*\$?([\d,]+(?:\.\d+)?)",
        coverage_text, _re.IGNORECASE,
    )
    if occ_match:
        per_occurrence = _parse_dollar_amount(occ_match.group(1))
        found_any = True

    agg_match = _re.search(
        r"(?:GENERAL AGGREGATE|AGGREGATE|GEN(?:ERAL)?\s*AGG)\s*\$?([\d,]+(?:\.\d+)?)",
        coverage_text, _re.IGNORECASE,
    )
    if agg_match:
        aggregate = _parse_dollar_amount(agg_match.group(1))
        found_any = True

    # Pattern 2: CSL / Combined Single Limit
    csl_match = _re.search(
        r"(?:COMBINED SINGLE LIMIT|CSL)\s*(?:\([^)]*\)\s*)?\$?([\d,]+(?:\.\d+)?)",
        coverage_text, _re.IGNORECASE,
    )
    if csl_match and not occ_match:
        per_occurrence = _parse_dollar_amount(csl_match.group(1))
        found_any = True

    # Pattern 3: shorthand like "$1,000,000 / $2,000,000" or "1M/2M"
    if not found_any:
        slash_match = _re.search(
            r"\$?([\d,]+(?:\.\d+)?[MmKk]?)\s*/\s*\$?([\d,]+(?:\.\d+)?[MmKk]?)",
            coverage_text,
        )
        if slash_match:
            per_occurrence = _parse_dollar_amount(slash_match.group(1))
            aggregate = _parse_dollar_amount(slash_match.group(2))
            found_any = True

    # Pattern 4: single number — treat as per-occurrence
    if not found_any:
        all_amounts = _re.findall(r"\$?([\d,]+(?:\.\d+)?)", coverage_text)
        valid = [_parse_dollar_amount(a) for a in all_amounts if _parse_dollar_amount(a) >= 1000]
        if valid:
            per_occurrence = valid[0]
            if len(valid) > 1:
                aggregate = max(valid)
            found_any = True

    return {
        "per_occurrence": per_occurrence,
        "aggregate": aggregate,
        "parse_ok": found_any and (per_occurrence > 0 or aggregate > 0),
    }


# ── Grace period ─────────────────────────────────────────────────────────────

def _parse_grace_period_from_notes(notes: str) -> int:
    """Extract grace period days from requirement Notes field. Default 30."""
    if not notes:
        return DEFAULT_GRACE_PERIOD_DAYS
    m = _re.search(r"grace\s*(?:period)?\s*[:=]?\s*(\d+)\s*days?", notes, _re.IGNORECASE)
    if m:
        return int(m.group(1))
    return DEFAULT_GRACE_PERIOD_DAYS


def _check_expiration(expiry_str: str, grace_days: int) -> str:
    """Returns 'current', 'grace_period', or 'expired'."""
    if not expiry_str:
        return "current"  # no date = can't determine, don't fail on expiration alone
    try:
        expiry = date.fromisoformat(expiry_str)
    except ValueError:
        return "current"
    today = date.today()
    if expiry >= today:
        return "current"
    days_past = (today - expiry).days
    if days_past <= grace_days:
        return "grace_period"
    return "expired"


# ── Per-line evaluation ──────────────────────────────────────────────────────

def _evaluate_line(req_fields, matching_policies, grace_days, insurance_policies_table=None,
                    overrides_table=None, vendor_id="", client_id=""):
    """Evaluate a single coverage line against its requirement.

    Returns (line_status, line_reasons) where line_reasons is a list of
    structured reason strings like "RC_CODE: human description".
    If an active exception exists for a deficiency, the deficiency is still
    recorded but annotated with the exception details.
    """
    policy_type = req_fields.get("Policy Type", "Unknown")
    required = req_fields.get("Required", False)
    min_limit = parse_limit(req_fields.get("Minimum Limit", "0"))
    ai_required = req_fields.get("Additional Insured Required", False)
    waiver_required = req_fields.get("Waiver Required", False)
    pnc_required = req_fields.get("Primary Noncontributory Required", False)

    # Missing policy
    if required and not matching_policies:
        return STATUS_MISSING_COVERAGE, [
            f"{RC_POLICY_MISSING}: {policy_type} — required but no policy on file"
        ]

    line_status = STATUS_COMPLIANT
    line_reasons = []

    def _worsen(new_status):
        nonlocal line_status
        if _STATUS_PRIORITY.get(new_status, 0) > _STATUS_PRIORITY.get(line_status, 0):
            line_status = new_status

    # Pick the best (most recent) policy for this type
    sorted_policies = sorted(
        matching_policies,
        key=lambda p: p["fields"].get("Expiration Date") or "",
        reverse=True,
    )
    best_policy = sorted_policies[0]
    pf = best_policy["fields"]

    # Expiration check with grace period
    expiry_str = pf.get("Expiration Date", "")
    exp_result = _check_expiration(expiry_str, grace_days)
    if exp_result == "expired":
        line_reasons.append(
            f"{RC_POLICY_EXPIRED}: {policy_type} expired on {expiry_str}"
        )
        _worsen(STATUS_EXPIRED)
    elif exp_result == "grace_period":
        line_reasons.append(
            f"{RC_POLICY_IN_GRACE_PERIOD}: {policy_type} expired {expiry_str} — within {grace_days}-day grace period"
        )
        _worsen(STATUS_NEEDS_REVIEW)

    # Coverage gap detection — is there a gap between policies?
    if len(sorted_policies) > 1:
        for i in range(len(sorted_policies) - 1):
            newer_eff = sorted_policies[i]["fields"].get("Effective Date", "")
            older_exp = sorted_policies[i + 1]["fields"].get("Expiration Date", "")
            if newer_eff and older_exp:
                try:
                    if date.fromisoformat(newer_eff) > date.fromisoformat(older_exp):
                        line_reasons.append(
                            f"{RC_COVERAGE_GAP_DETECTED}: {policy_type} gap between {older_exp} and {newer_eff}"
                        )
                        _worsen(STATUS_NEEDS_REVIEW)
                except ValueError:
                    pass

    # Limit comparison
    if min_limit > 0:
        limits = extract_limits_from_policy(best_policy)
        if not limits["parse_ok"]:
            line_reasons.append(
                f"{RC_LIMIT_PARSE_FAILED}: {policy_type} — could not parse coverage limits"
            )
            _worsen(STATUS_NEEDS_REVIEW)
        else:
            if limits["per_occurrence"] > 0 and limits["per_occurrence"] < min_limit:
                line_reasons.append(
                    f"{RC_LIMIT_BELOW_MIN_PER_OCCURRENCE}: {policy_type} per-occurrence "
                    f"${limits['per_occurrence']:,.0f} below required ${min_limit:,.0f}"
                )
                _worsen(STATUS_NEEDS_REVIEW)
            if limits["aggregate"] > 0 and limits["aggregate"] < min_limit:
                line_reasons.append(
                    f"{RC_LIMIT_BELOW_MIN_AGGREGATE}: {policy_type} aggregate "
                    f"${limits['aggregate']:,.0f} below required ${min_limit:,.0f}"
                )
                _worsen(STATUS_NEEDS_REVIEW)

    # ── Endorsement evaluation (evidence levels) ──────────────────────────
    has_ai = bool(pf.get("Additional Insured") or pf.get("additional_insured_checked"))
    has_wos = bool(pf.get("Waiver") or pf.get("waiver_of_subrogation_checked"))
    has_pnc = bool(pf.get("Primary Noncontributory") or pf.get("primary_noncontributory_checked"))

    # Write basic booleans to policy record
    if insurance_policies_table:
        try:
            insurance_policies_table.update(best_policy["id"], {
                FLD_P_AI_ON_FILE: has_ai,
                FLD_P_WOS_ON_FILE: has_wos,
            })
        except Exception:
            pass

    # Run endorsement evaluator for evidence-level analysis
    try:
        from endorsement_evaluator import evaluate_endorsements
        desc_of_ops = pf.get("description_of_operations") or pf.get("Description of Operations") or ""
        endorse_result = evaluate_endorsements(
            policy_fields=pf,
            requirement_fields=req_fields,
            description_of_operations=desc_of_ops,
        )
        for finding in endorse_result.findings:
            if finding.severity == "warning":
                line_reasons.append(f"{finding.reason_code}: {finding.message}")
        if endorse_result.needs_review:
            _worsen(STATUS_NEEDS_REVIEW)
    except Exception as exc:
        logger.warning("Endorsement evaluation failed for %s: %s", policy_type, exc)
        # Fall back to basic boolean checks
        if ai_required and not has_ai:
            line_reasons.append(f"{RC_AI_NOT_EVIDENCED}: {policy_type} — Additional Insured not evidenced")
            _worsen(STATUS_NEEDS_REVIEW)
        if waiver_required and not has_wos:
            line_reasons.append(f"{RC_WOS_NOT_EVIDENCED}: {policy_type} — Waiver of Subrogation not evidenced")
            _worsen(STATUS_NEEDS_REVIEW)
        if pnc_required and not has_pnc:
            line_reasons.append(f"{RC_PNC_NOT_EVIDENCED}: {policy_type} — Primary & Noncontributory not evidenced")
            _worsen(STATUS_NEEDS_REVIEW)

    # ── Claims-made evaluation ───────────────────────────────────────────
    policy_basis = (pf.get("Policy Basis") or pf.get("policy_basis") or "").strip().lower()
    if policy_basis == "claims-made":
        try:
            from claims_made_evaluator import evaluate_claims_made_policy
            prior_same_type = [p for p in sorted_policies[1:]]  # exclude best_policy
            cm_result = evaluate_claims_made_policy(pf, prior_same_type)
            for finding in cm_result.findings:
                line_reasons.append(f"{finding.reason_code}: {finding.message}")
            if cm_result.needs_review:
                _worsen(STATUS_NEEDS_REVIEW)
        except Exception as exc:
            logger.warning("Claims-made evaluation failed for %s: %s", policy_type, exc)

    # ── Exception lookup: annotate deficiencies with active exceptions ────
    if line_reasons and overrides_table and vendor_id and client_id:
        try:
            from exception_manager import find_active_exception, format_exception_note
            exception = find_active_exception(
                overrides_table, vendor_id, client_id, policy_type,
            )
            if exception:
                note = format_exception_note(exception)
                line_reasons.append(f"EXCEPTION_ACTIVE: {note}")
                logger.info("Active exception found for %s/%s/%s — deficiency annotated",
                            vendor_id, client_id, policy_type)
        except Exception as exc:
            logger.warning("Exception lookup failed for %s: %s", policy_type, exc)

    return line_status, line_reasons


# ── Main evaluation function ─────────────────────────────────────────────────

def evaluate_assignment(vendor, client_reqs, insurance_policies, assignment, assignments_table,
                        insurance_policies_table=None, overrides_table=None, client_id="",
                        certificate_holder_on_coi="", expected_certificate_holder=""):
    """Evaluate vendor compliance per-line and produce overall status + structured reasons."""
    vendor_id = vendor["id"]
    vendor_name = vendor["fields"].get("Vendor Name", vendor_id)
    logger.info("Evaluating assignment for vendor: %s", vendor_name)

    vendor_policies = [p for p in insurance_policies
                       if p["fields"].get("Vendor Link", [None])[0] == vendor_id]

    overall_status = STATUS_COMPLIANT
    all_reasons = []

    # Deduplicate requirements by policy type (take the strictest for each type)
    seen_types = {}
    for req in client_reqs:
        pt = req["fields"].get("Policy Type", "")
        if pt not in seen_types:
            seen_types[pt] = req
        else:
            # Keep the one with the higher minimum limit
            existing_limit = parse_limit(seen_types[pt]["fields"].get("Minimum Limit", "0"))
            new_limit = parse_limit(req["fields"].get("Minimum Limit", "0"))
            if new_limit > existing_limit:
                seen_types[pt] = req
            # Merge endorsement requirements (take True if either requires it)
            for field in ["Additional Insured Required", "Waiver Required", "Primary Noncontributory Required"]:
                if req["fields"].get(field):
                    seen_types[pt]["fields"][field] = True

    for policy_type, req in seen_types.items():
        matching = [p for p in vendor_policies if p["fields"].get("Policy Type") == policy_type]
        grace_days = _parse_grace_period_from_notes(req["fields"].get("Notes", ""))

        line_status, line_reasons = _evaluate_line(
            req["fields"], matching, grace_days, insurance_policies_table,
            overrides_table=overrides_table, vendor_id=vendor_id, client_id=client_id,
        )

        if _STATUS_PRIORITY.get(line_status, 0) > _STATUS_PRIORITY.get(overall_status, 0):
            overall_status = line_status
        all_reasons.extend(line_reasons)

        if line_reasons:
            logger.info("  %s → %s | %s", policy_type, line_status, "; ".join(line_reasons))
        else:
            logger.info("  %s → %s", policy_type, line_status)

    # ── Certificate holder comparison (once per assignment, not per line) ──
    if certificate_holder_on_coi and expected_certificate_holder:
        try:
            from endorsement_evaluator import evaluate_endorsements
            holder_result = evaluate_endorsements(
                policy_fields={"Policy Type": "Certificate"},
                requirement_fields={},
                certificate_holder_on_coi=certificate_holder_on_coi,
                expected_certificate_holder=expected_certificate_holder,
            )
            for finding in holder_result.findings:
                all_reasons.append(f"{finding.reason_code}: {finding.message}")
            if holder_result.needs_review:
                if _STATUS_PRIORITY.get(STATUS_NEEDS_REVIEW, 0) > _STATUS_PRIORITY.get(overall_status, 0):
                    overall_status = STATUS_NEEDS_REVIEW
        except Exception as exc:
            logger.warning("Holder comparison failed: %s", exc)

    # Write to assignment record
    try:
        assignments_table.update(assignment["id"], {
            "Compliance Status": overall_status,
            "Last Evaluated": date.today().isoformat()
        })
        logger.info("Vendor %s → %s | Reasons: %d", vendor_name, overall_status, len(all_reasons))
    except Exception as e:
        logger.error("Failed to update assignment: %s", e)

    return overall_status, all_reasons

COMPLIANCE_LOG_TABLE_ID = "tblxdt7DT6V3JCQcW"

# Table IDs used to resolve the most recent cert + vendor email for ladder wiring
INSURANCE_CERTS_TABLE_ID = "tbl0IH6zQQsXBff3l"
VENDORS_TABLE_ID_FOR_LADDER = "tblsOphSd5DKSZEro"

NONCOMPLIANT_DECISIONS = {"Non-Compliant", "Needs Review", "Missing Coverage"}


def _find_most_recent_cert_id_for_vendor(certs_table, vendor_id: str, client_id: str):
    """Find the most recent Insurance Certificate for this vendor (optionally
    filtered to the given client). Returns record id or None."""
    if not vendor_id:
        return None
    try:
        safe_v = vendor_id.replace("'", "\\'")
        formula = f"FIND('{safe_v}', ARRAYJOIN({{Vendor Link}}))"
        rows = certs_table.all(formula=formula)
    except Exception as exc:
        logger.warning("Cert lookup failed for vendor %s: %s", vendor_id, exc)
        return None
    if not rows:
        return None
    # Prefer certs linked to the same client when client_id is known
    if client_id:
        scoped = [r for r in rows if client_id in (r.get("fields", {}).get("Client Link") or [])]
        if scoped:
            rows = scoped
    rows.sort(
        key=lambda r: r.get("fields", {}).get("Certificate Date") or r.get("createdTime") or "",
        reverse=True,
    )
    return rows[0]["id"]


def _lookup_vendor_email(vendors_table, vendor_id: str) -> str:
    try:
        row = vendors_table.get(vendor_id)
        return (row.get("fields", {}).get("Vendor Email") or "").strip()
    except Exception:
        return ""


def trigger_post_evaluation_hooks(
    api, vendor_id, vendor_name, client_id, client_name,
    new_decision, previous_decision, failure_reasons,
):
    """After every compliance decision, run the three ladder-related hooks:
      1. Start a Noncompliance Return ladder if the new decision is noncompliant.
      2. Halt active ladders for this vendor if we just moved noncompliant → compliant.
      3. Run the short-dated check on the most recent cert.

    Failures are logged but never raised — compliance evaluation is the
    primary side effect and must not be rolled back by a downstream issue.
    """
    if not api or not vendor_id:
        return

    try:
        certs_table = api.table(AIRTABLE_BASE_ID, INSURANCE_CERTS_TABLE_ID)
        cert_id = _find_most_recent_cert_id_for_vendor(certs_table, vendor_id, client_id)
    except Exception as exc:
        logger.warning("trigger_post_evaluation_hooks: cert table access failed: %s", exc)
        cert_id = None

    # 1. Noncompliance Return ladder
    if new_decision in NONCOMPLIANT_DECISIONS:
        try:
            from module_22_followup_ladder import create_ladder, LADDER_TYPE_NONCOMPLIANCE
            from module_15_email_queue_builder import build_deficiency_email_body

            vendors_table = api.table(AIRTABLE_BASE_ID, VENDORS_TABLE_ID_FOR_LADDER)
            vendor_email = _lookup_vendor_email(vendors_table, vendor_id)

            if vendor_email:
                subject = f"Documentation Update Needed: {vendor_name} — {client_name}"
                body = build_deficiency_email_body(vendor_name, client_name or "your client", failure_reasons or [])
                create_ladder(
                    cert_id=cert_id,
                    ladder_type=LADDER_TYPE_NONCOMPLIANCE,
                    email_subject=subject,
                    email_body=body,
                    original_sender=vendor_email,
                    vendor_id=vendor_id,
                    client_id=client_id,
                    named_insured=vendor_name,
                    trigger_context=f"Compliance decision: {new_decision}",
                    api=api,
                )
            else:
                logger.info(
                    "Ladder trigger skipped for vendor %s (%s) — no Vendor Email on file",
                    vendor_name, vendor_id,
                )
        except Exception as exc:
            logger.warning("Noncompliance ladder trigger failed for %s: %s", vendor_name, exc)

    # 2. Halt ladders when transitioning to Compliant
    elif (
        new_decision == STATUS_COMPLIANT
        and previous_decision in NONCOMPLIANT_DECISIONS
    ):
        try:
            from module_22_followup_ladder import halt_ladders_for_vendor
            halt_ladders_for_vendor(vendor_id, reason="New compliant cert received", api=api)
        except Exception as exc:
            logger.warning("halt_ladders_for_vendor failed for %s: %s", vendor_name, exc)

    # 3. Short-dated check (independent of compliance decision)
    if cert_id:
        try:
            from module_20_short_dated_check import check_and_trigger_short_dated
            check_and_trigger_short_dated(cert_id, api=api)
        except Exception as exc:
            logger.warning("Short-dated check failed for cert %s: %s", cert_id, exc)


def write_compliance_log(vendor_id, vendor_name, client_id, client_name,
                         new_decision, previous_decision, failure_reasons,
                         compliance_log_table):
    """Write one record to the Compliance Log table after every compliance decision."""
    if not vendor_id:
        logger.info("Skipping compliance log — no vendor ID")
        return
    if not client_id:
        logger.info("Skipping compliance log — no client ID")
        return
    try:
        log_date = date.today().isoformat()
        log_entry = f"{vendor_name} — {client_name} — {log_date}"
        decision_changed = (new_decision != previous_decision)

        record = {
            "fldzUTll7izbXfBEB": log_entry,                          # Log Entry (primary)
            "fldWzdPiDQOXqlwhg": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),  # Timestamp
            "fldu8DcdwPbDxCvpb": vendor_name,                        # Vendor Name
            "fldcbzhSPWdOBanym": client_name,                        # Client Name
            "fld6Ml1MK3QpmIICK": new_decision,                       # Decision
            "fldoTDHEEkekT2jAj": "\n".join(failure_reasons) if failure_reasons else "",  # Failure Reasons
            "fld4HwkZfiYTwVnW6": previous_decision or "",            # Previous Decision
            "fld7lWbXuLhKg46Dk": decision_changed,                   # Decision Changed
            "fldkq8vZZsN6uBTL0": [vendor_id],                       # Vendor Link
            "fldbKVYBi20p6wykZ": [client_id],                        # Client Link
        }

        compliance_log_table.create(record)
        logger.info("Compliance log written: %s → %s (changed: %s)",
                     log_entry, new_decision, decision_changed)
    except Exception as e:
        logger.error("Failed to write compliance log for %s / %s: %s",
                      vendor_name, client_name, e)


def run():
    logger.info("=== Module 7B: Requirement Validator starting ===")

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        logger.error("Airtable API credentials are missing.")
        return

    api = Api(AIRTABLE_API_KEY)
    vendors_table = api.table(AIRTABLE_BASE_ID, TABLE_VENDORS)
    client_requirements_table = api.table(AIRTABLE_BASE_ID, TABLE_CLIENT_REQUIREMENTS)
    insurance_policies_table = api.table(AIRTABLE_BASE_ID, TABLE_INSURANCE_POLICIES)
    assignments_table = api.table(AIRTABLE_BASE_ID, "Vendor Client Assignments")
    clients_table = api.table(AIRTABLE_BASE_ID, "tbltnBIWke20IEI3K")  # Clients table
    compliance_log_table = api.table(AIRTABLE_BASE_ID, COMPLIANCE_LOG_TABLE_ID)
    overrides_table = api.table(AIRTABLE_BASE_ID, "tblKVIdTYwF1Sp5Iw")  # Vendor Requirement Overrides
    vendors = fetch_records(vendors_table)

    for vendor in vendors:
        vendor_id = vendor["id"]
        vendor_name = vendor["fields"].get("Vendor Name", vendor_id)

        # Fetch active assignments for the vendor
        active_assignments = fetch_active_assignments_for_vendor(vendor_id, assignments_table)

        if active_assignments:
            for assignment in active_assignments:
                client_id = assignment["fields"].get("Client Link", [None])[0]
                if not client_id:
                    logger.warning("Assignment for vendor %s has no linked client", vendor_name)
                    continue

                # Fetch client record to get name and check requirements status
                try:
                    client_record = clients_table.get(client_id)
                    client_name = client_record["fields"].get("Client Name", client_id)
                except Exception as e:
                    logger.error("Failed to fetch client record %s: %s", client_id, e)
                    continue

                # Guard: skip client if no requirements are configured
                client_reqs = fetch_requirements_for_client(client_id, client_requirements_table)
                if not client_reqs:
                    logger.info("Client %s has no requirements configured — skipping.", client_name)
                    continue

                # Guard: skip client if Requirements Status is not "Received"
                # Clients table: tbltnBIWke20IEI3K | Field ID: flde7mzXePBqKBjGx
                requirements_status = client_record["fields"].get("Requirements Status")
                if requirements_status != "Received":
                    logger.info("Client %s requirements not yet confirmed received — skipping.", client_name)
                    continue

                # Fetch vendor policies
                vendor_policies = fetch_policies_for_vendor(vendor_id, insurance_policies_table)

                # Capture previous decision before evaluating
                previous_decision = assignment["fields"].get("Compliance Status", "")

                # Resolve expected certificate holder from client record
                expected_holder = client_record["fields"].get("Certificate Holder") or ""

                # Evaluate assignment
                new_decision, failure_reasons = evaluate_assignment(
                    vendor, client_reqs, vendor_policies, assignment, assignments_table,
                    insurance_policies_table=insurance_policies_table,
                    overrides_table=overrides_table, client_id=client_id,
                    expected_certificate_holder=expected_holder,
                )

                # Update vendor-level Expiration Status rollup
                update_vendor_expiration_status(vendors_table, vendor_id, vendor_name, vendor_policies)

                # Update vendor-level policy detail (GL/WC/Auto status, AI, WOS, next expiration)
                update_vendor_policy_detail(vendors_table, vendor_id, vendor_name, vendor_policies)

                # Write to compliance log
                write_compliance_log(
                    vendor_id=vendor_id,
                    vendor_name=vendor_name,
                    client_id=client_id,
                    client_name=client_name,
                    new_decision=new_decision,
                    previous_decision=previous_decision,
                    failure_reasons=failure_reasons,
                    compliance_log_table=compliance_log_table,
                )

                # Ladder + short-dated hooks (module 20/22)
                trigger_post_evaluation_hooks(
                    api=api,
                    vendor_id=vendor_id,
                    vendor_name=vendor_name,
                    client_id=client_id,
                    client_name=client_name,
                    new_decision=new_decision,
                    previous_decision=previous_decision,
                    failure_reasons=failure_reasons,
                )
        else:
            # Use legacy path
            client_id_legacy = vendor["fields"].get("Client Link", [None])[0]
            client_requirements = fetch_requirements_for_client(client_id_legacy, client_requirements_table)
            insurance_policies = fetch_policies_for_vendor(vendor_id, insurance_policies_table)

            # Capture previous decision
            previous_decision_legacy = vendor["fields"].get("Compliance Status", "")

            status = validate_vendor(vendor, client_requirements, insurance_policies)
            update_vendor_status(vendors_table, vendor_id, status)

            # Update vendor-level Expiration Status rollup
            update_vendor_expiration_status(vendors_table, vendor_id, vendor_name, insurance_policies)

            # Update vendor-level policy detail (GL/WC/Auto status, AI, WOS, next expiration)
            update_vendor_policy_detail(vendors_table, vendor_id, vendor_name, insurance_policies)

            # Write to compliance log (legacy path — no specific client name available)
            legacy_client_name = ""
            if client_id_legacy:
                try:
                    client_rec = api.table(AIRTABLE_BASE_ID, "tbltnBIWke20IEI3K").get(client_id_legacy)
                    legacy_client_name = client_rec["fields"].get("Client Name", "")
                except Exception:
                    legacy_client_name = client_id_legacy or ""

            write_compliance_log(
                vendor_id=vendor_id,
                vendor_name=vendor_name,
                client_id=client_id_legacy or "",
                client_name=legacy_client_name,
                new_decision=status,
                previous_decision=previous_decision_legacy,
                failure_reasons=[],  # Legacy path does not produce granular reasons
                compliance_log_table=compliance_log_table,
            )

            # Ladder + short-dated hooks (legacy path — no structured reasons)
            trigger_post_evaluation_hooks(
                api=api,
                vendor_id=vendor_id,
                vendor_name=vendor_name,
                client_id=client_id_legacy or "",
                client_name=legacy_client_name,
                new_decision=status,
                previous_decision=previous_decision_legacy,
                failure_reasons=[],
            )

    logger.info("=== Requirement validation complete ===")

if __name__ == "__main__":
    run()