"""
Policy Timing Evaluator — Carolina Compliance Solutions

Evaluates timing-related compliance signals after a new certificate is created:
- Future-dated COI handling (Pending Active)
- Coverage gap detection between old and new policies
- Backdated correction detection
- Policy term length validation
- Same-day expiration handling (clean renewal)

Called from processor.py after certificate + policy creation, before compliance evaluation.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Airtable field IDs ───────────────────────────────────────────────────────
FLD_TIMING_FLAGS        = "fldw4m0Y3flkWbYzB"    # Insurance Policies: Timing Flags
FLD_POLICY_TERM_MONTHS  = "fld5zRAnYenj5Sy52"    # Insurance Policies: Policy Term Months
FLD_EXPIRATION_STATUS   = "fldXLb8RLrLc56bBx"    # Insurance Policies: Expiration Status
FLD_EXPIRATION_DATE     = "fldMXXlcJ6BTuEpZk"    # Insurance Policies: Expiration Date
FLD_EFFECTIVE_DATE      = "fldvj6n6aR0btY1U1"    # Insurance Policies: Effective Date

FLD_CERT_COMPLIANCE_SUMMARY = "fldX7L6Goy7LomDMo"  # Insurance Certificates: Compliance Decision Summary
FLD_CERT_COMPLIANCE_STATUS  = "fldvy3UFvZq1D57tn"   # Insurance Certificates: Compliance Status

# ── Timing flag codes ────────────────────────────────────────────────────────
FLAG_FUTURE_EFFECTIVE        = "FUTURE_EFFECTIVE"
FLAG_COVERAGE_GAP            = "COVERAGE_GAP_DETECTED"
FLAG_BACKDATED_CORRECTION    = "BACKDATED_CORRECTION"
FLAG_SHORT_POLICY_TERM       = "SHORT_POLICY_TERM"
FLAG_MONTHLY_POLICY          = "MONTHLY_POLICY_DETECTED"
FLAG_CLEAN_RENEWAL           = "CLEAN_RENEWAL"
FLAG_SAME_DAY_RENEWAL        = "SAME_DAY_RENEWAL"


# ── Date helpers ─────────────────────────────────────────────────────────────

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


def _months_between(d1: date, d2: date) -> int:
    """Approximate months between two dates."""
    return (d2.year - d1.year) * 12 + (d2.month - d1.month)


# ── Timing evaluation result ─────────────────────────────────────────────────

@dataclass
class TimingFinding:
    flag: str
    message: str
    severity: str = "info"  # info, warning, error


@dataclass
class PolicyTimingResult:
    findings: List[TimingFinding] = field(default_factory=list)
    pending_active: bool = False
    has_gap: bool = False
    needs_human_review: bool = False
    term_months: Optional[int] = None

    @property
    def flags_text(self) -> str:
        return "\n".join(f"{f.flag}: {f.message}" for f in self.findings)

    @property
    def summary_text(self) -> str:
        if not self.findings:
            return "No timing issues detected."
        lines = []
        for f in self.findings:
            prefix = "WARNING" if f.severity == "warning" else "INFO" if f.severity == "info" else "ERROR"
            lines.append(f"[{prefix}] {f.flag}: {f.message}")
        return "\n".join(lines)


# ── Core evaluation functions ────────────────────────────────────────────────

def evaluate_policy_timing(
    new_policy_fields: Dict[str, Any],
    existing_policies_same_type: List[Dict[str, Any]],
) -> PolicyTimingResult:
    """Evaluate timing signals for a single new policy against existing policies of the same type."""
    result = PolicyTimingResult()
    today = date.today()

    effective = _parse_date(
        new_policy_fields.get("Effective Date") or new_policy_fields.get(FLD_EFFECTIVE_DATE) or ""
    )
    expiration = _parse_date(
        new_policy_fields.get("Expiration Date") or new_policy_fields.get(FLD_EXPIRATION_DATE) or ""
    )
    policy_type = new_policy_fields.get("Policy Type", "Unknown")

    # ── Term length check ────────────────────────────────────────────────
    if effective and expiration and expiration > effective:
        term = _months_between(effective, expiration)
        result.term_months = term

        if term == 1:
            result.findings.append(TimingFinding(
                flag=FLAG_MONTHLY_POLICY,
                message=f"{policy_type} has a 1-month term ({effective} to {expiration}). Monthly policies require more frequent monitoring.",
                severity="warning",
            ))
        elif term < 10:
            result.findings.append(TimingFinding(
                flag=FLAG_SHORT_POLICY_TERM,
                message=f"{policy_type} term is {term} months ({effective} to {expiration}). Standard is 12 months.",
                severity="info",
            ))

    # ── Future-dated COI ─────────────────────────────────────────────────
    if effective and effective > today:
        result.pending_active = True
        days_until = (effective - today).days
        result.findings.append(TimingFinding(
            flag=FLAG_FUTURE_EFFECTIVE,
            message=f"{policy_type} effective date {effective} is {days_until} days in the future. Status set to Pending Active until effective.",
            severity="info",
        ))

    # ── Coverage gap detection against existing policies ─────────────────
    if effective and existing_policies_same_type:
        # Find the most recent prior policy (by expiration date)
        prior_policies = []
        for ep in existing_policies_same_type:
            ep_exp = _parse_date(
                ep["fields"].get("Expiration Date") or ep["fields"].get(FLD_EXPIRATION_DATE) or ""
            )
            ep_status = ep["fields"].get("Status", "")
            if ep_exp and ep_status != "Superseded":
                prior_policies.append((ep, ep_exp))

        if prior_policies:
            prior_policies.sort(key=lambda x: x[1], reverse=True)
            prior_record, prior_exp = prior_policies[0]

            gap_days = (effective - prior_exp).days

            if gap_days == 0:
                # Same-day renewal — clean
                result.findings.append(TimingFinding(
                    flag=FLAG_SAME_DAY_RENEWAL,
                    message=f"{policy_type} effective {effective} matches prior expiration {prior_exp}. Clean renewal, no gap.",
                    severity="info",
                ))
            elif gap_days < 0:
                # Overlapping coverage — check if this is a backdated correction
                if effective < today and prior_exp < today:
                    result.needs_human_review = True
                    result.findings.append(TimingFinding(
                        flag=FLAG_BACKDATED_CORRECTION,
                        message=f"{policy_type} effective {effective} is backdated into a period already covered by prior policy expiring {prior_exp}. Requires human review — do not automatically clear lapse.",
                        severity="warning",
                    ))
                else:
                    result.findings.append(TimingFinding(
                        flag=FLAG_CLEAN_RENEWAL,
                        message=f"{policy_type} has overlapping coverage ({effective} to {prior_exp}). No gap.",
                        severity="info",
                    ))
            else:
                # Gap detected
                result.has_gap = True
                result.findings.append(TimingFinding(
                    flag=FLAG_COVERAGE_GAP,
                    message=f"{policy_type} has a {gap_days}-day coverage gap between prior expiration {prior_exp} and new effective {effective}.",
                    severity="warning",
                ))

    return result


def evaluate_certificate_timing(
    policies_created: List[Dict[str, Any]],
    all_vendor_policies: List[Dict[str, Any]],
) -> PolicyTimingResult:
    """Evaluate timing for all policies on a newly created certificate.

    Groups by policy type, evaluates each against existing policies of the same type.
    Returns an aggregate result.
    """
    aggregate = PolicyTimingResult()

    # Group existing policies by type
    existing_by_type: Dict[str, List[Dict[str, Any]]] = {}
    for p in all_vendor_policies:
        pt = p["fields"].get("Policy Type", "")
        if pt not in existing_by_type:
            existing_by_type[pt] = []
        existing_by_type[pt].append(p)

    for new_policy in policies_created:
        new_fields = new_policy.get("fields", new_policy)
        policy_type = new_fields.get("Policy Type", "")
        new_id = new_policy.get("id", "")

        # Existing policies of same type, excluding the new one
        existing_same = [
            p for p in existing_by_type.get(policy_type, [])
            if p.get("id") != new_id
        ]

        result = evaluate_policy_timing(new_fields, existing_same)

        aggregate.findings.extend(result.findings)
        if result.pending_active:
            aggregate.pending_active = True
        if result.has_gap:
            aggregate.has_gap = True
        if result.needs_human_review:
            aggregate.needs_human_review = True
        if result.term_months is not None:
            aggregate.term_months = result.term_months

    return aggregate


# ── Airtable writers ─────────────────────────────────────────────────────────

def write_timing_to_policy(policies_table, policy_id: str, result: PolicyTimingResult):
    """Write timing flags and term length to an Insurance Policy record."""
    fields = {}

    if result.flags_text:
        fields[FLD_TIMING_FLAGS] = result.flags_text
    if result.term_months is not None:
        fields[FLD_POLICY_TERM_MONTHS] = result.term_months
    if result.pending_active:
        fields[FLD_EXPIRATION_STATUS] = "Pending Active"

    if fields:
        try:
            policies_table.update(policy_id, fields, typecast=True)
            logger.info("Timing flags written to policy %s: %s",
                        policy_id, result.flags_text[:100] if result.flags_text else "(none)")
        except Exception as e:
            logger.error("Failed to write timing flags to policy %s: %s", policy_id, e)


def write_timing_to_certificate(certs_table, certificate_id: str, result: PolicyTimingResult):
    """Append timing summary to the Compliance Decision Summary on the certificate."""
    if not result.findings:
        return

    try:
        # Read existing summary
        cert = certs_table.get(certificate_id)
        existing_summary = cert.get("fields", {}).get("Compliance Decision Summary") or ""

        timing_block = "\n--- Timing Evaluation ---\n" + result.summary_text
        new_summary = (existing_summary + timing_block).strip()

        update_fields = {FLD_CERT_COMPLIANCE_SUMMARY: new_summary}

        # If there's a gap or needs human review, set certificate status
        if result.has_gap or result.needs_human_review:
            update_fields[FLD_CERT_COMPLIANCE_STATUS] = "Needs Review"

        certs_table.update(certificate_id, update_fields)
        logger.info("Timing summary written to certificate %s (%d findings)",
                     certificate_id, len(result.findings))
    except Exception as e:
        logger.error("Failed to write timing to certificate %s: %s", certificate_id, e)
