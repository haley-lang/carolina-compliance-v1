"""
Endorsement Evaluator — Carolina Compliance Solutions

Evaluates endorsement documentation evidence on submitted certificates:
- Evidence levels (checkbox only vs description reference vs attached endorsement)
- Additional Insured, Waiver of Subrogation, Primary & Noncontributory reconciliation
- Blanket vs scheduled endorsement detection
- Certificate holder mismatch detection
- Endorsement date mismatch detection

IMPORTANT: This module surfaces certificate documentation findings for GC review.
It does not determine whether endorsements exist on the actual policy, only what
is evidenced on the submitted certificate documentation.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Airtable field IDs ───────────────────────────────────────────────────────
FLD_AI_EVIDENCE_LEVEL   = "fld8pTTHFEbipAxg2"
FLD_WOS_EVIDENCE_LEVEL  = "fldobP5C2CX3kGORP"
FLD_PNC_EVIDENCE_LEVEL  = "flde7UUCXzzNtgyXy"
FLD_DESC_OF_OPERATIONS  = "fldwNsc8W3qHF8CoT"
FLD_ENDORSEMENT_FLAGS   = "fldeit6q3YlC68bDI"

FLD_CERT_COMPLIANCE_SUMMARY = "fldX7L6Goy7LomDMo"
FLD_CERT_COMPLIANCE_STATUS  = "fldvy3UFvZq1D57tn"

FLD_CLIENT_CERT_HOLDER  = "fldNkyxgVJjg6bzdm"

# ── Evidence levels ──────────────────────────────────────────────────────────
LEVEL_1_CHECKBOX = "Level 1 - Checkbox Only"
LEVEL_2_DESCRIPTION = "Level 2 - Description Reference"
LEVEL_3_ENDORSEMENT = "Level 3 - Endorsement Attached"
LEVEL_NONE = None

# ── Reason codes ─────────────────────────────────────────────────────────────
RC_AI_CHECKBOX_ONLY         = "AI_CHECKBOX_ONLY"
RC_AI_EVIDENCED             = "AI_EVIDENCED"
RC_WOS_CHECKBOX_ONLY        = "WOS_CHECKBOX_ONLY"
RC_WOS_EVIDENCED            = "WOS_EVIDENCED"
RC_PNC_NOT_IN_DESCRIPTION   = "PNC_NOT_IN_DESCRIPTION"
RC_PNC_EVIDENCED            = "PNC_EVIDENCED"
RC_BLANKET_AI_DETECTED      = "BLANKET_AI_DETECTED"
RC_SCHEDULED_AI_DETECTED    = "SCHEDULED_AI_DETECTED"
RC_HOLDER_MISMATCH          = "CERTIFICATE_HOLDER_MISMATCH"
RC_ENDORSEMENT_DATE_MISMATCH = "ENDORSEMENT_DATE_MISMATCH"

# ── Patterns ─────────────────────────────────────────────────────────────────
AI_DESC_PATTERNS = [
    r"additional\s+insured",
    r"add(?:'?l|itional)\s+ins(?:ured|d)",
    r"named\s+as\s+additional\s+insured",
]

WOS_DESC_PATTERNS = [
    r"waiver\s+of\s+subrogation",
    r"subrogation\s+(?:is\s+)?waived",
    r"waiver\s+of\s+transfer\s+of\s+rights",
]

PNC_DESC_PATTERNS = [
    r"primary\s+and\s+non-?contributory",
    r"primary\s*[/&]\s*non-?contributory",
    r"primary\s+basis",
]

BLANKET_PATTERNS = [
    r"blanket\s+additional\s+insured",
    r"blanket\s+add(?:'?l|itional)\s+ins",
    r"blanket\s+endorsement",
]

_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y")


def _parse_date(raw: str) -> Optional[date]:
    cleaned = (raw or "").strip()
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _matches_any(text: str, patterns: list) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in patterns)


def _normalize_holder(text: str) -> str:
    """Normalize a certificate holder name for comparison.
    Uses the shared normalize_holder_name from utils.py.
    """
    from utils import normalize_holder_name
    return normalize_holder_name(text)


# ── Finding dataclass ────────────────────────────────────────────────────────

@dataclass
class EndorsementFinding:
    reason_code: str
    message: str
    severity: str = "info"  # info, warning


@dataclass
class EndorsementResult:
    findings: List[EndorsementFinding] = field(default_factory=list)
    needs_review: bool = False
    ai_level: Optional[str] = None
    wos_level: Optional[str] = None
    pnc_level: Optional[str] = None

    @property
    def flags_text(self) -> str:
        return "\n".join(f"{f.reason_code}: {f.message}" for f in self.findings)

    @property
    def summary_text(self) -> str:
        if not self.findings:
            return ""
        lines = []
        for f in self.findings:
            prefix = "WARNING" if f.severity == "warning" else "INFO"
            lines.append(f"[{prefix}] {f.reason_code}: {f.message}")
        return "\n".join(lines)


# ── Evidence level determination ─────────────────────────────────────────────

def _determine_ai_evidence_level(
    checkbox_checked: bool,
    description_text: str,
    has_endorsement_doc: bool,
) -> Optional[str]:
    if has_endorsement_doc:
        return LEVEL_3_ENDORSEMENT
    if checkbox_checked and _matches_any(description_text, AI_DESC_PATTERNS):
        return LEVEL_2_DESCRIPTION
    if checkbox_checked:
        return LEVEL_1_CHECKBOX
    return LEVEL_NONE


def _determine_wos_evidence_level(
    checkbox_checked: bool,
    description_text: str,
    has_endorsement_doc: bool,
) -> Optional[str]:
    if has_endorsement_doc:
        return LEVEL_3_ENDORSEMENT
    if checkbox_checked and _matches_any(description_text, WOS_DESC_PATTERNS):
        return LEVEL_2_DESCRIPTION
    if checkbox_checked:
        return LEVEL_1_CHECKBOX
    return LEVEL_NONE


def _determine_pnc_evidence_level(
    checkbox_checked: bool,
    description_text: str,
    has_endorsement_doc: bool,
) -> Optional[str]:
    if has_endorsement_doc:
        return LEVEL_3_ENDORSEMENT
    if _matches_any(description_text, PNC_DESC_PATTERNS):
        return LEVEL_2_DESCRIPTION
    if checkbox_checked:
        return LEVEL_1_CHECKBOX
    return LEVEL_NONE


# ── Core evaluation ──────────────────────────────────────────────────────────

def evaluate_endorsements(
    policy_fields: Dict[str, Any],
    requirement_fields: Dict[str, Any],
    description_of_operations: str = "",
    has_endorsement_doc: bool = False,
    certificate_holder_on_coi: str = "",
    expected_certificate_holder: str = "",
    endorsement_effective_date: str = "",
    policy_effective_date: str = "",
) -> EndorsementResult:
    """Evaluate endorsement documentation evidence for a single policy line.

    Args:
        policy_fields: the policy record fields
        requirement_fields: the client requirement fields for this policy type
        description_of_operations: text from the Description of Operations block
        has_endorsement_doc: True if an actual endorsement document was attached
        certificate_holder_on_coi: the certificate holder name from the COI
        expected_certificate_holder: the client's expected Certificate Holder name
        endorsement_effective_date: effective date on attached endorsement (if any)
        policy_effective_date: effective date on the policy
    """
    result = EndorsementResult()
    policy_type = policy_fields.get("Policy Type", "Unknown")
    desc = description_of_operations or ""

    ai_required = requirement_fields.get("Additional Insured Required", False)
    wos_required = requirement_fields.get("Waiver Required", False)
    pnc_required = requirement_fields.get("Primary Noncontributory Required", False)

    # Read by Airtable display name. policy_fields is name-keyed (pyairtable
    # default). The display names "Additional Insured On File" and "Waiver of
    # Subrogation On File" are verified live against the Insurance Policies
    # schema — the snake_case JSON keys never reach this dict because the
    # values arrive on the policy record via processor.py's field-ID writes.
    ai_checked = bool(policy_fields.get("Additional Insured On File"))
    wos_checked = bool(policy_fields.get("Waiver of Subrogation On File"))
    pnc_checked = bool(policy_fields.get("Primary Noncontributory"))

    # ── AI evidence level ────────────────────────────────────────────────
    result.ai_level = _determine_ai_evidence_level(ai_checked, desc, has_endorsement_doc)

    if ai_required:
        if result.ai_level == LEVEL_3_ENDORSEMENT:
            result.findings.append(EndorsementFinding(
                reason_code=RC_AI_EVIDENCED,
                message=(
                    f"Additional insured endorsement document provided for {policy_type}. "
                    f"Endorsement documentation is on file."
                ),
                severity="info",
            ))
        elif result.ai_level == LEVEL_2_DESCRIPTION:
            result.findings.append(EndorsementFinding(
                reason_code=RC_AI_CHECKBOX_ONLY,
                message=(
                    f"Additional insured is indicated on submitted {policy_type} certificate "
                    f"and referenced in the description of operations. No separate endorsement "
                    f"document has been provided. Please request the endorsement from the "
                    f"insured or their broker for your records."
                ),
                severity="info",
            ))
        elif result.ai_level == LEVEL_1_CHECKBOX:
            result.findings.append(EndorsementFinding(
                reason_code=RC_AI_CHECKBOX_ONLY,
                message=(
                    f"Additional insured is indicated on submitted {policy_type} certificate "
                    f"(checkbox only) but no endorsement document has been provided and the "
                    f"description of operations does not reference the endorsement. "
                    f"Please request the endorsement from the insured or their broker."
                ),
                severity="warning",
            ))
            result.needs_review = True
        else:
            result.findings.append(EndorsementFinding(
                reason_code=RC_AI_CHECKBOX_ONLY,
                message=(
                    f"Additional insured is required for {policy_type} but is not indicated "
                    f"on the submitted certificate documentation. Please request an updated "
                    f"certificate showing additional insured status."
                ),
                severity="warning",
            ))
            result.needs_review = True

    # ── WOS evidence level ───────────────────────────────────────────────
    result.wos_level = _determine_wos_evidence_level(wos_checked, desc, has_endorsement_doc)

    if wos_required:
        if result.wos_level == LEVEL_3_ENDORSEMENT:
            result.findings.append(EndorsementFinding(
                reason_code=RC_WOS_EVIDENCED,
                message=(
                    f"Waiver of subrogation endorsement document provided for {policy_type}. "
                    f"Endorsement documentation is on file."
                ),
                severity="info",
            ))
        elif result.wos_level == LEVEL_2_DESCRIPTION:
            result.findings.append(EndorsementFinding(
                reason_code=RC_WOS_CHECKBOX_ONLY,
                message=(
                    f"Waiver of subrogation is indicated on submitted {policy_type} certificate "
                    f"and referenced in the description of operations. No separate endorsement "
                    f"document has been provided. Please request the endorsement from the "
                    f"insured or their broker for your records."
                ),
                severity="info",
            ))
        elif result.wos_level == LEVEL_1_CHECKBOX:
            result.findings.append(EndorsementFinding(
                reason_code=RC_WOS_CHECKBOX_ONLY,
                message=(
                    f"Waiver of subrogation is indicated on submitted {policy_type} certificate "
                    f"(checkbox only) but no endorsement document has been provided and the "
                    f"description of operations does not reference the waiver. "
                    f"Please request the endorsement from the insured or their broker."
                ),
                severity="warning",
            ))
            result.needs_review = True
        else:
            result.findings.append(EndorsementFinding(
                reason_code=RC_WOS_CHECKBOX_ONLY,
                message=(
                    f"Waiver of subrogation is required for {policy_type} but is not indicated "
                    f"on the submitted certificate documentation. Please request an updated "
                    f"certificate showing waiver of subrogation."
                ),
                severity="warning",
            ))
            result.needs_review = True

    # ── PNC evidence level ───────────────────────────────────────────────
    result.pnc_level = _determine_pnc_evidence_level(pnc_checked, desc, has_endorsement_doc)

    if pnc_required:
        if result.pnc_level in (LEVEL_2_DESCRIPTION, LEVEL_3_ENDORSEMENT):
            result.findings.append(EndorsementFinding(
                reason_code=RC_PNC_EVIDENCED,
                message=(
                    f"Primary and noncontributory wording is evidenced on submitted "
                    f"{policy_type} certificate documentation."
                ),
                severity="info",
            ))
        else:
            result.findings.append(EndorsementFinding(
                reason_code=RC_PNC_NOT_IN_DESCRIPTION,
                message=(
                    f"Primary and noncontributory wording is not evidenced on the submitted "
                    f"{policy_type} certificate. Please request written confirmation from "
                    f"the broker."
                ),
                severity="warning",
            ))
            result.needs_review = True

    # ── Blanket vs scheduled AI detection ────────────────────────────────
    if ai_required and ai_checked and desc:
        if _matches_any(desc, BLANKET_PATTERNS):
            result.findings.append(EndorsementFinding(
                reason_code=RC_BLANKET_AI_DETECTED,
                message=(
                    f"Certificate documentation for {policy_type} references blanket additional "
                    f"insured language. Please confirm with your advisor whether this "
                    f"satisfies your contract requirements."
                ),
                severity="info",
            ))
        else:
            # Check if a specific entity is named
            if expected_certificate_holder:
                holder_norm = _normalize_holder(expected_certificate_holder)
                desc_norm = _normalize_holder(desc)
                if holder_norm and holder_norm in desc_norm:
                    result.findings.append(EndorsementFinding(
                        reason_code=RC_SCHEDULED_AI_DETECTED,
                        message=(
                            f"Certificate documentation for {policy_type} names "
                            f"'{expected_certificate_holder}' in the description of operations."
                        ),
                        severity="info",
                    ))

    # ── Certificate holder comparison (fuzzy) ────────────────────────────
    if certificate_holder_on_coi and expected_certificate_holder:
        coi_norm = _normalize_holder(certificate_holder_on_coi)
        expected_norm = _normalize_holder(expected_certificate_holder)

        if coi_norm and expected_norm:
            try:
                from rapidfuzz import fuzz
                score = fuzz.token_sort_ratio(coi_norm, expected_norm)
            except ImportError:
                # Fallback to exact match if rapidfuzz not available
                score = 100.0 if coi_norm == expected_norm else 0.0

            if score >= 95:
                # Exact or near-exact match — pass silently
                pass
            elif score >= 80:
                # Soft match — pass but note the near-match
                result.findings.append(EndorsementFinding(
                    reason_code="HOLDER_NEAR_MATCH",
                    message=(
                        f"Note: Certificate holder name is a near-match. "
                        f"Printed on COI: '{certificate_holder_on_coi}'. "
                        f"Expected: '{expected_certificate_holder}'. "
                        f"Please verify if needed."
                    ),
                    severity="info",
                ))
            else:
                # Below 80% — mismatch, flag for review
                result.findings.append(EndorsementFinding(
                    reason_code=RC_HOLDER_MISMATCH,
                    message=(
                        f"Certificate holder on submitted documentation "
                        f"('{certificate_holder_on_coi}') does not closely match "
                        f"the expected holder ('{expected_certificate_holder}'). "
                        f"Please verify this certificate was issued for the "
                        f"correct project."
                    ),
                    severity="warning",
                ))
                result.needs_review = True

    # ── Endorsement date mismatch ────────────────────────────────────────
    if endorsement_effective_date and policy_effective_date:
        end_date = _parse_date(endorsement_effective_date)
        pol_date = _parse_date(policy_effective_date)
        if end_date and pol_date and end_date != pol_date:
            result.findings.append(EndorsementFinding(
                reason_code=RC_ENDORSEMENT_DATE_MISMATCH,
                message=(
                    f"Endorsement effective date ({endorsement_effective_date}) does not match "
                    f"the policy effective date ({policy_effective_date}) on submitted "
                    f"documentation. Please review with your insurance advisor."
                ),
                severity="warning",
            ))
            result.needs_review = True

    return result


# ── Airtable writers ─────────────────────────────────────────────────────────

def write_endorsement_to_policy(policies_table, policy_id: str, result: EndorsementResult,
                                 description_of_operations: str = ""):
    """Write endorsement evidence levels and flags to the Insurance Policy record."""
    fields = {}

    if result.ai_level:
        fields[FLD_AI_EVIDENCE_LEVEL] = result.ai_level
    if result.wos_level:
        fields[FLD_WOS_EVIDENCE_LEVEL] = result.wos_level
    if result.pnc_level:
        fields[FLD_PNC_EVIDENCE_LEVEL] = result.pnc_level
    if description_of_operations:
        fields[FLD_DESC_OF_OPERATIONS] = description_of_operations[:5000]
    if result.flags_text:
        fields[FLD_ENDORSEMENT_FLAGS] = result.flags_text

    if fields:
        try:
            policies_table.update(policy_id, fields, typecast=True)
            logger.info("Endorsement evidence written to policy %s: AI=%s WOS=%s PNC=%s flags=%d",
                        policy_id,
                        result.ai_level or "(none)",
                        result.wos_level or "(none)",
                        result.pnc_level or "(none)",
                        len(result.findings))
        except Exception as e:
            logger.error("Failed to write endorsement evidence to policy %s: %s", policy_id, e)


def write_endorsement_to_certificate(certs_table, certificate_id: str, result: EndorsementResult):
    """Append endorsement findings to the certificate's compliance summary."""
    if not result.findings:
        return

    try:
        cert = certs_table.get(certificate_id)
        existing = cert.get("fields", {}).get("Compliance Decision Summary") or ""

        endorsement_block = "\n--- Endorsement Evaluation ---\n" + result.summary_text
        new_summary = (existing + endorsement_block).strip()

        update_fields = {FLD_CERT_COMPLIANCE_SUMMARY: new_summary}
        if result.needs_review:
            update_fields[FLD_CERT_COMPLIANCE_STATUS] = "Needs Review"

        certs_table.update(certificate_id, update_fields)
        logger.info("Endorsement findings written to certificate %s (%d findings)",
                     certificate_id, len(result.findings))
    except Exception as e:
        logger.error("Failed to write endorsement findings to certificate %s: %s", certificate_id, e)
