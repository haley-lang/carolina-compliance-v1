"""
Claims-Made Policy Evaluator — Carolina Compliance Solutions

Evaluates claims-made policy documentation signals:
- Retro date continuity between certificates
- Carrier changes on claims-made renewals
- Occurrence vs claims-made basis switches
- Tail coverage documentation on cancellations

IMPORTANT: This module surfaces certificate documentation findings for GC review.
It does not determine coverage adequacy or policy validity. All output language
uses "certificate documentation indicates..." framing.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Airtable field IDs ───────────────────────────────────────────────────────
FLD_POLICY_BASIS            = "fldPMR5nVSExiH2h2"
FLD_RETRO_DATE              = "fldHSPP6979xM7OC6"
FLD_TAIL_COVERAGE           = "fldZz5vGdz9vDaxz0"
FLD_CLAIMS_MADE_FLAGS       = "fldNqC6hneXapVPuL"

FLD_CERT_TIMING_SUMMARY     = "fldxDHuqTWx5jSgCW"
FLD_CERT_COMPLIANCE_SUMMARY = "fldX7L6Goy7LomDMo"
FLD_CERT_COMPLIANCE_STATUS  = "fldvy3UFvZq1D57tn"

# ── Reason codes ─────────────────────────────────────────────────────────────
RC_RETRO_DATE_MOVED_FORWARD       = "CLAIMS_MADE_RETRO_DATE_MOVED_FORWARD"
RC_CARRIER_CHANGE_CLAIMS_MADE     = "CLAIMS_MADE_CARRIER_CHANGE"
RC_BASIS_CHANGED                  = "POLICY_BASIS_CHANGED"
RC_NO_TAIL_COVERAGE_EVIDENCED     = "NO_TAIL_COVERAGE_EVIDENCED"
RC_RETRO_DATE_MISSING             = "CLAIMS_MADE_RETRO_DATE_MISSING"
RC_BASIS_UNKNOWN                  = "POLICY_BASIS_UNKNOWN"

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


# ── Finding dataclass ────────────────────────────────────────────────────────

@dataclass
class ClaimsMadeFinding:
    reason_code: str
    message: str
    severity: str = "warning"  # info, warning


@dataclass
class ClaimsMadeResult:
    findings: List[ClaimsMadeFinding] = field(default_factory=list)
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
            lines.append(f"[{f.severity.upper()}] {f.reason_code}: {f.message}")
        return "\n".join(lines)


# ── Core evaluation ──────────────────────────────────────────────────────────

def evaluate_claims_made_policy(
    new_policy_fields: Dict[str, Any],
    prior_policies_same_type: List[Dict[str, Any]],
    is_cancellation: bool = False,
) -> ClaimsMadeResult:
    """Evaluate claims-made documentation signals for a single policy line.

    Args:
        new_policy_fields: fields dict for the new policy being evaluated
        prior_policies_same_type: existing policies of the same type for this vendor
            (excluding the new one), sorted most-recent first
        is_cancellation: True if this evaluation is triggered by a cancellation notice
    """
    result = ClaimsMadeResult()
    policy_type = new_policy_fields.get("Policy Type", "Unknown")

    new_basis = (
        new_policy_fields.get("Policy Basis")
        or new_policy_fields.get(FLD_POLICY_BASIS)
        or "unknown"
    ).strip().lower()

    # ── Unknown basis detection ──────────────────────────────────────────
    if new_basis == "unknown" or not new_basis:
        result.findings.append(ClaimsMadeFinding(
            reason_code=RC_BASIS_UNKNOWN,
            message=(
                f"Certificate documentation for {policy_type} does not clearly indicate "
                f"whether this is a claims-made or occurrence policy. "
                f"Please review the submitted certificate with your insurance advisor."
            ),
            severity="warning",
        ))
        result.needs_review = True
        return result

    # Not claims-made — check for basis switch only
    if new_basis != "claims-made":
        # Check if prior was claims-made (basis switch)
        for prior in prior_policies_same_type:
            prior_basis = (
                prior["fields"].get("Policy Basis")
                or prior["fields"].get(FLD_POLICY_BASIS)
                or ""
            ).strip().lower()
            if prior_basis == "claims-made":
                result.findings.append(ClaimsMadeFinding(
                    reason_code=RC_BASIS_CHANGED,
                    message=(
                        f"Certificate documentation for {policy_type} indicates the policy basis "
                        f"changed from claims-made to occurrence between certificates. "
                        f"Please review with your insurance advisor to confirm prior acts coverage."
                    ),
                    severity="warning",
                ))
                result.needs_review = True
                break
        return result

    # ── Claims-made specific checks ──────────────────────────────────────

    new_retro_raw = (
        new_policy_fields.get("Retroactive Date")
        or new_policy_fields.get(FLD_RETRO_DATE)
        or new_policy_fields.get("retro_date")
        or ""
    )
    new_retro = _parse_date(str(new_retro_raw))
    new_carrier = (new_policy_fields.get("Carrier") or "").strip().lower()

    # Missing retro date on claims-made
    if not new_retro:
        result.findings.append(ClaimsMadeFinding(
            reason_code=RC_RETRO_DATE_MISSING,
            message=(
                f"Certificate documentation for {policy_type} indicates claims-made basis "
                f"but no retroactive date is shown on the submitted certificate. "
                f"Please request the retroactive date from the insured or their broker."
            ),
            severity="warning",
        ))
        result.needs_review = True

    # Check for basis switch (occurrence → claims-made) against ALL priors
    for prior in prior_policies_same_type:
        prior_basis = (
            prior["fields"].get("Policy Basis")
            or prior["fields"].get(FLD_POLICY_BASIS)
            or ""
        ).strip().lower()
        if prior_basis == "occurrence":
            result.findings.append(ClaimsMadeFinding(
                reason_code=RC_BASIS_CHANGED,
                message=(
                    f"Certificate documentation for {policy_type} indicates the policy basis "
                    f"changed from occurrence to claims-made between certificates. "
                    f"Please review with your insurance advisor."
                ),
                severity="warning",
            ))
            result.needs_review = True
            break

    # Compare against prior claims-made policies for retro/carrier checks
    prior_claims_made = [
        p for p in prior_policies_same_type
        if (p["fields"].get("Policy Basis") or p["fields"].get(FLD_POLICY_BASIS) or "")
           .strip().lower() == "claims-made"
    ]

    if prior_claims_made:
        # Sort by expiration descending to get most recent prior
        prior_claims_made.sort(
            key=lambda p: p["fields"].get("Expiration Date") or "",
            reverse=True,
        )
        most_recent_prior = prior_claims_made[0]
        prior_fields = most_recent_prior["fields"]

        # Retro date continuity check
        prior_retro_raw = (
            prior_fields.get("Retroactive Date")
            or prior_fields.get(FLD_RETRO_DATE)
            or ""
        )
        prior_retro = _parse_date(str(prior_retro_raw))

        if new_retro and prior_retro and new_retro > prior_retro:
            result.findings.append(ClaimsMadeFinding(
                reason_code=RC_RETRO_DATE_MOVED_FORWARD,
                message=(
                    f"Retroactive date on submitted {policy_type} certificate ({new_retro}) "
                    f"is later than the retroactive date on the prior certificate ({prior_retro}). "
                    f"Please review with your insurance advisor to confirm continuity of "
                    f"prior acts coverage."
                ),
                severity="warning",
            ))
            result.needs_review = True

        # Carrier change check
        prior_carrier = (prior_fields.get("Carrier") or "").strip().lower()
        if new_carrier and prior_carrier and new_carrier != prior_carrier:
            result.findings.append(ClaimsMadeFinding(
                reason_code=RC_CARRIER_CHANGE_CLAIMS_MADE,
                message=(
                    f"Certificate documentation for {policy_type} indicates the carrier "
                    f"changed from '{prior_fields.get('Carrier', '')}' to "
                    f"'{new_policy_fields.get('Carrier', '')}' on a claims-made policy. "
                    f"Please confirm prior acts coverage with your insurance advisor."
                ),
                severity="warning",
            ))
            result.needs_review = True

        # Basis switch (occurrence → claims-made) already checked above against all priors

    # ── Tail coverage check on cancellation ──────────────────────────────
    if is_cancellation:
        tail_evidenced = bool(
            new_policy_fields.get("Tail Coverage Evidenced")
            or new_policy_fields.get(FLD_TAIL_COVERAGE)
            or new_policy_fields.get("tail_coverage_evidenced")
        )
        if not tail_evidenced:
            result.findings.append(ClaimsMadeFinding(
                reason_code=RC_NO_TAIL_COVERAGE_EVIDENCED,
                message=(
                    f"Cancellation notice received for claims-made {policy_type} policy. "
                    f"No tail coverage (extended reporting period) is evidenced on the "
                    f"submitted certificate documentation. Please confirm extended reporting "
                    f"period arrangements with your insurance advisor."
                ),
                severity="warning",
            ))
            result.needs_review = True

    return result


# ── Airtable writers ─────────────────────────────────────────────────────────

def write_claims_made_to_policy(policies_table, policy_id: str, policy_fields: Dict[str, Any],
                                 result: ClaimsMadeResult):
    """Write claims-made fields to the Insurance Policy record."""
    fields = {}

    basis = (
        policy_fields.get("Policy Basis")
        or policy_fields.get("policy_basis")
        or "unknown"
    ).strip().lower()
    if basis in ("claims-made", "occurrence", "unknown"):
        fields[FLD_POLICY_BASIS] = basis

    retro = policy_fields.get("Retroactive Date") or policy_fields.get("retro_date") or ""
    if retro:
        fields[FLD_RETRO_DATE] = str(retro).strip()

    tail = (
        policy_fields.get("Tail Coverage Evidenced")
        or policy_fields.get("tail_coverage_evidenced")
    )
    if tail is not None:
        fields[FLD_TAIL_COVERAGE] = bool(tail)

    if result.flags_text:
        fields[FLD_CLAIMS_MADE_FLAGS] = result.flags_text

    if fields:
        try:
            policies_table.update(policy_id, fields, typecast=True)
            logger.info("Claims-made fields written to policy %s: basis=%s retro=%s flags=%d",
                        policy_id, basis, retro or "(none)", len(result.findings))
        except Exception as e:
            logger.error("Failed to write claims-made fields to policy %s: %s", policy_id, e)


def write_claims_made_to_certificate(certs_table, certificate_id: str, result: ClaimsMadeResult):
    """Append claims-made findings to the certificate's compliance summary."""
    if not result.findings:
        return

    try:
        cert = certs_table.get(certificate_id)
        existing_summary = cert.get("fields", {}).get("Compliance Decision Summary") or ""

        claims_block = "\n--- Claims-Made Evaluation ---\n" + result.summary_text
        new_summary = (existing_summary + claims_block).strip()

        update_fields = {FLD_CERT_COMPLIANCE_SUMMARY: new_summary}

        if result.needs_review:
            update_fields[FLD_CERT_COMPLIANCE_STATUS] = "Needs Review"

        certs_table.update(certificate_id, update_fields)
        logger.info("Claims-made findings written to certificate %s (%d findings)",
                     certificate_id, len(result.findings))
    except Exception as e:
        logger.error("Failed to write claims-made findings to certificate %s: %s", certificate_id, e)
