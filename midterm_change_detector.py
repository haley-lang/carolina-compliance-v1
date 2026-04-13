"""
Mid-Term Change Detector — Carolina Compliance Solutions

Compares a newly submitted certificate against the prior accepted certificate
for the same vendor and flags material changes:
- Carrier changed
- Limits reduced
- Named insured changed
- Endorsement removed (AI, WOS, PNC)
- Cancellation date vs certificate expiry conflict

IMPORTANT: All findings are framed as certificate documentation observations.
The system surfaces changes for the GC to review. It never auto-accepts
mid-term changes or determines coverage adequacy.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Airtable field IDs ───────────────────────────────────────────────────────
FLD_CERT_MIDTERM_CHANGES    = "fld6F2PJ5xySOLTpE"
FLD_CERT_COMPLIANCE_SUMMARY = "fldX7L6Goy7LomDMo"
FLD_CERT_COMPLIANCE_STATUS  = "fldvy3UFvZq1D57tn"
FLD_CERT_NAMED_INSURED      = "fldVvH4NGg69t8BZL"

# ── Reason codes ─────────────────────────────────────────────────────────────
RC_CARRIER_CHANGED        = "CARRIER_CHANGED"
RC_LIMITS_REDUCED         = "LIMITS_REDUCED"
RC_NAMED_INSURED_CHANGED  = "NAMED_INSURED_CHANGED"
RC_ENDORSEMENT_REMOVED    = "ENDORSEMENT_REMOVED"
RC_CANCEL_DATE_CONFLICT   = "CANCELLATION_DATE_EXPIRY_CONFLICT"

_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y")


def _parse_date(raw) -> Optional[date]:
    cleaned = (str(raw) if raw else "").strip()
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _parse_dollar(text: str) -> float:
    text = (text or "").replace("$", "").replace(",", "").strip()
    m = re.match(r"^([\d.]+)\s*[Mm]$", text)
    if m:
        return float(m.group(1)) * 1_000_000
    try:
        return float(text)
    except (ValueError, TypeError):
        return 0.0


def _extract_max_limit(coverage_text: str) -> float:
    """Extract the highest dollar amount from a coverage limits string."""
    if not coverage_text:
        return 0.0
    amounts = re.findall(r"\$?([\d,]+(?:\.\d+)?)", coverage_text)
    values = [_parse_dollar(a) for a in amounts if _parse_dollar(a) >= 1000]
    return max(values) if values else 0.0


def _normalize(text: str) -> str:
    return (text or "").strip().upper()


# ── Finding dataclass ────────────────────────────────────────────────────────

@dataclass
class MidTermFinding:
    reason_code: str
    message: str
    severity: str = "warning"


@dataclass
class MidTermResult:
    findings: List[MidTermFinding] = field(default_factory=list)
    needs_review: bool = False

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


# ── Core comparison ──────────────────────────────────────────────────────────

def compare_policies(
    new_policy: Dict[str, Any],
    prior_policy: Dict[str, Any],
) -> List[MidTermFinding]:
    """Compare a new policy against a prior policy of the same type.
    Returns a list of findings for material changes.
    """
    findings = []
    new_f = new_policy.get("fields", new_policy)
    prior_f = prior_policy.get("fields", prior_policy)
    policy_type = new_f.get("Policy Type", "Unknown")

    # Carrier change
    new_carrier = _normalize(new_f.get("Carrier"))
    prior_carrier = _normalize(prior_f.get("Carrier"))
    if new_carrier and prior_carrier and new_carrier != prior_carrier:
        findings.append(MidTermFinding(
            reason_code=RC_CARRIER_CHANGED,
            message=(
                f"Certificate documentation shows carrier changed on {policy_type} "
                f"from '{prior_f.get('Carrier', '')}' to '{new_f.get('Carrier', '')}'. "
                f"Please review with your insurance advisor."
            ),
        ))

    # Limits reduced
    new_limits = _extract_max_limit(new_f.get("Coverage Limits", ""))
    prior_limits = _extract_max_limit(prior_f.get("Coverage Limits", ""))
    if new_limits > 0 and prior_limits > 0 and new_limits < prior_limits:
        findings.append(MidTermFinding(
            reason_code=RC_LIMITS_REDUCED,
            message=(
                f"Certificate documentation shows {policy_type} limits decreased "
                f"from ${prior_limits:,.0f} to ${new_limits:,.0f}. "
                f"Please verify this still meets your requirements."
            ),
        ))

    # AI removed
    prior_ai = bool(prior_f.get("Additional Insured") or prior_f.get("additional_insured_checked"))
    new_ai = bool(new_f.get("Additional Insured") or new_f.get("additional_insured_checked"))
    if prior_ai and not new_ai:
        findings.append(MidTermFinding(
            reason_code=RC_ENDORSEMENT_REMOVED,
            message=(
                f"Certificate documentation no longer shows Additional Insured on {policy_type} "
                f"that was present on the prior certificate. "
                f"Please follow up with the vendor."
            ),
        ))

    # WOS removed
    prior_wos = bool(prior_f.get("Waiver") or prior_f.get("waiver_of_subrogation_checked"))
    new_wos = bool(new_f.get("Waiver") or new_f.get("waiver_of_subrogation_checked"))
    if prior_wos and not new_wos:
        findings.append(MidTermFinding(
            reason_code=RC_ENDORSEMENT_REMOVED,
            message=(
                f"Certificate documentation no longer shows Waiver of Subrogation on {policy_type} "
                f"that was present on the prior certificate. "
                f"Please follow up with the vendor."
            ),
        ))

    # PNC removed
    prior_pnc = bool(prior_f.get("Primary Noncontributory") or prior_f.get("primary_noncontributory_checked"))
    new_pnc = bool(new_f.get("Primary Noncontributory") or new_f.get("primary_noncontributory_checked"))
    if prior_pnc and not new_pnc:
        findings.append(MidTermFinding(
            reason_code=RC_ENDORSEMENT_REMOVED,
            message=(
                f"Certificate documentation no longer shows Primary & Noncontributory on {policy_type} "
                f"that was present on the prior certificate. "
                f"Please follow up with the vendor."
            ),
        ))

    return findings


def detect_midterm_changes(
    new_policies: List[Dict[str, Any]],
    prior_policies: List[Dict[str, Any]],
    new_named_insured: str = "",
    prior_named_insured: str = "",
    cancellation_effective_date: str = "",
    certificate_expiry_date: str = "",
) -> MidTermResult:
    """Compare new certificate policies against prior certificate policies.

    Groups by policy type and compares the most recent prior policy of each type
    against the new policy of the same type.
    """
    result = MidTermResult()

    # Named insured change (certificate level)
    new_ni = _normalize(new_named_insured)
    prior_ni = _normalize(prior_named_insured)
    if new_ni and prior_ni and new_ni != prior_ni:
        result.findings.append(MidTermFinding(
            reason_code=RC_NAMED_INSURED_CHANGED,
            message=(
                f"Certificate documentation shows named insured changed "
                f"from '{prior_named_insured}' to '{new_named_insured}'. "
                f"Please verify this is the correct entity."
            ),
        ))
        result.needs_review = True

    # Cancellation date vs certificate expiry conflict
    if cancellation_effective_date and certificate_expiry_date:
        cancel_date = _parse_date(cancellation_effective_date)
        expiry_date = _parse_date(certificate_expiry_date)
        if cancel_date and expiry_date and cancel_date < expiry_date:
            result.findings.append(MidTermFinding(
                reason_code=RC_CANCEL_DATE_CONFLICT,
                message=(
                    f"Cancellation effective date on submitted notice ({cancellation_effective_date}) "
                    f"is earlier than the expiration date on the accepted certificate "
                    f"({certificate_expiry_date}). Please review the timeline with your "
                    f"insurance advisor."
                ),
            ))
            result.needs_review = True

    # Group prior policies by type
    prior_by_type: Dict[str, List[Dict[str, Any]]] = {}
    for p in prior_policies:
        pt = (p.get("fields", p)).get("Policy Type", "")
        if pt not in prior_by_type:
            prior_by_type[pt] = []
        prior_by_type[pt].append(p)

    # Compare each new policy against the most recent prior of the same type
    for new_p in new_policies:
        new_f = new_p.get("fields", new_p)
        pt = new_f.get("Policy Type", "")
        if not pt or pt not in prior_by_type:
            continue

        # Pick most recent prior by expiration date
        priors = prior_by_type[pt]
        priors.sort(
            key=lambda p: (p.get("fields", p)).get("Expiration Date", ""),
            reverse=True,
        )
        prior_p = priors[0]

        line_findings = compare_policies(new_p, prior_p)
        result.findings.extend(line_findings)

    if result.findings:
        result.needs_review = True

    return result


# ── Airtable writer ──────────────────────────────────────────────────────────

def write_midterm_changes_to_certificate(certs_table, certificate_id: str, result: MidTermResult):
    """Write mid-term change findings to the certificate record."""
    if not result.findings:
        return

    try:
        cert = certs_table.get(certificate_id)
        existing_summary = cert.get("fields", {}).get("Compliance Decision Summary") or ""

        changes_block = "\n--- Mid-Term Change Detection ---\n" + result.summary_text
        new_summary = (existing_summary + changes_block).strip()

        update_fields = {
            FLD_CERT_COMPLIANCE_SUMMARY: new_summary,
            FLD_CERT_MIDTERM_CHANGES: result.flags_text,
        }

        if result.needs_review:
            update_fields[FLD_CERT_COMPLIANCE_STATUS] = "Needs Review"

        certs_table.update(certificate_id, update_fields, typecast=True)
        logger.info("Mid-term changes written to certificate %s (%d findings)",
                     certificate_id, len(result.findings))
    except Exception as e:
        logger.error("Failed to write mid-term changes to certificate %s: %s", certificate_id, e)
