"""Returned COI compliance evaluator interface (v1 stub).

Purpose:
- Define a stable interface that Module 4 (`processor.py`) can call after
  certificate/policy creation.
- Keep behavior isolated and non-invasive while compliance comparison logic is
  still pending.

This module intentionally does NOT implement requirement comparison yet.

Design notes (v1 persistence target):
- Primary write target for compliance evaluation output is `Insurance Certificates`.
- This aligns outcome data to the specific returned COI/certificate record that was
  evaluated, rather than storing client-specific compliance state globally on
  `Vendors`.
- `Client Vendors` may be used later for rollups/denormalized summaries, but it is
  NOT the v1 source of truth.

Recommended Airtable fields for v1 (on `Insurance Certificates`):
- `Compliance Status` (single select: Compliant | Non-Compliant | Needs Review)
- `Compliance Evaluated At` (date-time)
- `Compliance Decision Summary` (long text)
- `Compliance Failure Reasons` (long text/json)
- Optional: `Deficiency Email Queue Status` (single select/text)

Example write payload shape (v1 target = Insurance Certificates):
{
  "insurance_certificate_id": "recCERT123",
  "fields": {
    "Compliance Status": "Needs Review",
    "Compliance Evaluated At": "2026-03-31T20:15:00Z",
    "Compliance Decision Summary": "Compliance evaluator interface stub executed; logic not implemented.",
    "Compliance Failure Reasons": "[{\"code\":\"EVALUATOR_NOT_IMPLEMENTED\",\"message\":\"Compliance comparison logic is not implemented yet.\",\"severity\":\"info\"}]",
    "Deficiency Email Queue Status": "Not Queued"
  }
}
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from dataclasses import dataclass, field as dc_field
from typing import Any, Dict, List, Literal, Optional
import pytz


logger = logging.getLogger(__name__)

eastern = pytz.timezone("America/New_York")


def _current_et_timestamp_str() -> str:
    return (
        datetime.utcnow()
        .replace(tzinfo=pytz.utc)
        .astimezone(eastern)
        .strftime("%Y-%m-%d %I:%M:%S %p ET")
    )


# V1 outcome set requested by product requirements.
ComplianceOutcome = Literal["Compliant", "Non-Compliant", "Needs Review"]


@dataclass(frozen=True)
class ComplianceFailureReason:
    """Structured reason container for future deficiency handling."""

    code: str
    message: str
    severity: Literal["info", "warning", "error"] = "warning"
    field: Optional[str] = None
    policy_id: Optional[str] = None
    metadata: Dict[str, Any] = dc_field(default_factory=dict)


@dataclass(frozen=True)
class ReturnedCoiComplianceInput:
    """Input payload available at the post-processing insertion point."""

    extraction_id: str
    vendor_id: str
    client_id: Optional[str]
    request_id: Optional[str]
    certificate_id: str
    policy_ids: List[str]
    document_type: str
    source_filename: str
    named_insured: str
    raw_data: Dict[str, Any] = dc_field(default_factory=dict)


@dataclass(frozen=True)
class ReturnedCoiComplianceResult:
    """Evaluator output contract for downstream writes and future queueing."""

    outcome: ComplianceOutcome
    decision_summary: str
    failure_reasons: List[ComplianceFailureReason] = dc_field(default_factory=list)
    evaluator_version: str = "v1-interface-stub"

    # Future queue compatibility (deficiency email orchestration can read these).
    should_queue_deficiency_email: bool = False
    deficiency_email_payload: Optional[Dict[str, Any]] = None

    # Safe place for additional machine-readable outputs later.
    metadata: Dict[str, Any] = dc_field(default_factory=dict)


_POLICY_TYPE_MAP = {
    "commercial general liability": "General Liability",
    "general liability": "General Liability",
    "workers compensation": "Workers Compensation",
    "workers comp": "Workers Compensation",
    "workers compenstation": "Workers Compensation",
    "automobile liability": "Auto Liability",
    "auto liability": "Auto Liability",
    "commercial auto": "Auto Liability",
    "umbrella liability": "Umbrella",
    "umbrella liab": "Umbrella",
    "umbrella": "Umbrella",
    "excess liability": "Umbrella",
}


def _normalize_policy_type(raw_value: Any) -> Optional[str]:
    value = str(raw_value or "").strip().lower()
    if not value:
        return None
    value = re.sub(r"\s+", " ", value)
    return _POLICY_TYPE_MAP.get(value, value.title())


def _extract_required_policy_types(raw_data: Dict[str, Any]) -> tuple[set[str], str]:
    required_policy_requirements, source = _extract_required_policy_requirements(raw_data)
    return set(required_policy_requirements.keys()), source


def _extract_required_policy_requirements(raw_data: Dict[str, Any]) -> tuple[Dict[str, Dict[str, Any]], str]:
    """Best-effort extraction of required policy types from linked request/client context.

    Source priority (safest-first):
      1) request-linked requirement arrays
      2) client-linked requirement arrays
      3) generic requirement arrays
    """
    source_candidates = [
        "matched_client_request.requirements",
        "client_request.requirements",
        "request.requirements",
        "request_requirements",
        "matched_client.requirements",
        "client.requirements",
        "client_requirements",
        "requirements",
    ]

    def _get_by_path(data: Dict[str, Any], dotted_path: str) -> Any:
        current: Any = data
        for segment in dotted_path.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(segment)
        return current

    for source_path in source_candidates:
        records = _get_by_path(raw_data, source_path)
        if not isinstance(records, list):
            continue

        required_requirements: Dict[str, Dict[str, Any]] = {}
        for item in records:
            if not isinstance(item, dict):
                continue

            required_flag = item.get("Required")
            if required_flag is None:
                required_flag = item.get("required")
            if required_flag is False:
                continue

            raw_policy_type = (
                item.get("Policy Type")
                or item.get("policy_type")
                or item.get("policyType")
                or item.get("type")
            )
            normalized = _normalize_policy_type(raw_policy_type)
            if not normalized:
                continue

            minimum_limit_raw = (
                item.get("Minimum Limit")
                or item.get("minimum_limit")
                or item.get("minimumLimit")
                or item.get("min_limit")
            )
            minimum_limit_value = _parse_requirement_minimum_limit(minimum_limit_raw)
            required_limit_type = _extract_requirement_limit_type(item)

            existing = required_requirements.get(normalized)
            if existing is None:
                required_requirements[normalized] = {
                    "required_policy_type": normalized,
                    "minimum_limit_raw": minimum_limit_raw,
                    "minimum_limit_value": minimum_limit_value,
                    "required_limit_type": required_limit_type,
                }
            elif (
                minimum_limit_value is not None
                and (
                    existing.get("minimum_limit_value") is None
                    or float(minimum_limit_value) > float(existing["minimum_limit_value"])
                )
            ):
                # If duplicate requirement rows exist for a type, keep the strictest minimum.
                existing["minimum_limit_raw"] = minimum_limit_raw
                existing["minimum_limit_value"] = minimum_limit_value
                existing["required_limit_type"] = required_limit_type

        if required_requirements:
            return required_requirements, source_path

    return {}, "none"


def _extract_processed_policy_types(raw_data: Dict[str, Any]) -> set[str]:
    processed_types: set[str] = set()

    policy_rows = raw_data.get("policies")
    if isinstance(policy_rows, list):
        for row in policy_rows:
            if not isinstance(row, dict):
                continue
            normalized = _normalize_policy_type(
                row.get("policy_type") or row.get("Policy Type") or row.get("type")
            )
            if normalized:
                processed_types.add(normalized)

    direct_list_candidates = [
        raw_data.get("policy_types"),
        raw_data.get("processed_policy_types"),
    ]
    for candidate in direct_list_candidates:
        if not isinstance(candidate, list):
            continue
        for raw_value in candidate:
            normalized = _normalize_policy_type(raw_value)
            if normalized:
                processed_types.add(normalized)

    return processed_types


def _parse_policy_expiration_date(raw_value: Any) -> Optional[date]:
    value = str(raw_value or "").strip()
    if not value:
        return None

    # Common extracted/normalized formats seen in pipeline output.
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    try:
        # Handles full ISO timestamps like 2026-03-31T00:00:00
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def _extract_numeric_values(raw_value: Any) -> List[float]:
    if raw_value is None:
        return []
    if isinstance(raw_value, (int, float)):
        return [float(raw_value)]

    text = str(raw_value).strip()
    if not text:
        return []

    text = text.replace(",", "")
    matches = re.findall(r"\d+(?:\.\d+)?", text)
    values: List[float] = []
    for match in matches:
        try:
            values.append(float(match))
        except ValueError:
            continue
    return values


def _normalize_limit_type(raw_value: Any) -> str:
    """Normalize requirement limit targeting to canonical tokens.

    Returns one of:
      - each_occurrence
      - aggregate
      - any (fallback)
    """
    text = str(raw_value or "").strip().lower()
    if not text:
        return "any"

    normalized_text = re.sub(r"\s+", " ", text)
    compact_text = normalized_text.replace("-", " ")

    if any(token in compact_text for token in ["each occurrence", "per occurrence", "occurrence"]):
        return "each_occurrence"
    if any(token in compact_text for token in ["aggregate", "agg"]):
        return "aggregate"

    return "any"


def _extract_requirement_limit_type(requirement_item: Dict[str, Any]) -> str:
    """Infer which policy limit bucket a requirement is targeting."""
    limit_type_candidates = [
        requirement_item.get("Limit Type"),
        requirement_item.get("limit_type"),
        requirement_item.get("limitType"),
        requirement_item.get("Applies To"),
        requirement_item.get("applies_to"),
        requirement_item.get("appliesTo"),
        requirement_item.get("Minimum Limit"),
        requirement_item.get("minimum_limit"),
        requirement_item.get("minimumLimit"),
    ]

    for candidate in limit_type_candidates:
        normalized = _normalize_limit_type(candidate)
        if normalized != "any":
            return normalized

    return "any"


def _normalize_policy_limit_label(label: str) -> Optional[str]:
    text = re.sub(r"\s+", " ", str(label or "").strip().lower())
    if not text:
        return None

    if any(token in text for token in ["each occurrence", "per occurrence", "occurrence"]):
        return "each_occurrence"
    if any(token in text for token in ["aggregate", "agg"]):
        return "aggregate"

    return None


def _extract_structured_policy_limits(raw_value: Any) -> Dict[str, float]:
    """Parse common COI limit strings into canonical limit buckets."""
    structured_limits: Dict[str, float] = {}

    if raw_value is None:
        return structured_limits

    if isinstance(raw_value, dict):
        for key, value in raw_value.items():
            normalized_key = _normalize_policy_limit_label(str(key))
            parsed_values = _extract_numeric_values(value)
            if normalized_key and parsed_values:
                structured_limits[normalized_key] = max(
                    structured_limits.get(normalized_key, 0.0),
                    max(parsed_values),
                )
        return structured_limits

    text = str(raw_value or "").strip()
    if not text:
        return structured_limits

    pair_pattern = re.compile(
        r"([A-Za-z][A-Za-z0-9\s/&\-\.\(\)]{2,}?)\s*[:\-]\s*\$?([\d,]+(?:\.\d+)?)",
        flags=re.IGNORECASE,
    )

    for label, amount_raw in pair_pattern.findall(text):
        normalized_label = _normalize_policy_limit_label(label)
        numeric_values = _extract_numeric_values(amount_raw)
        if normalized_label and numeric_values:
            structured_limits[normalized_label] = max(
                structured_limits.get(normalized_label, 0.0),
                max(numeric_values),
            )

    return structured_limits


def _parse_requirement_minimum_limit(raw_value: Any) -> Optional[float]:
    values = _extract_numeric_values(raw_value)
    if not values:
        return None
    # Requirement minimum limit should be a single target amount; pick the first parseable token.
    return values[0]


def _extract_expired_required_policies(
    raw_data: Dict[str, Any],
    required_policy_types: set[str],
) -> List[Dict[str, Any]]:
    """Find required policy rows that are present but expired.

    Expired signal source:
      - expiration date parses and is earlier than today
    """
    expired: List[Dict[str, Any]] = []
    # All compliance date comparisons use UTC to avoid Airtable/local timezone inconsistencies
    today = datetime.utcnow().date()

    policy_rows = raw_data.get("policies")
    if not isinstance(policy_rows, list):
        return expired

    for row in policy_rows:
        if not isinstance(row, dict):
            continue

        normalized_type = _normalize_policy_type(
            row.get("policy_type") or row.get("Policy Type") or row.get("type")
        )
        if not normalized_type or normalized_type not in required_policy_types:
            continue

        expiration_raw = (
            row.get("expiration_date")
            or row.get("Expiration Date")
            or row.get("expiry_date")
            or row.get("expires_on")
            or ""
        )
        parsed_expiration = _parse_policy_expiration_date(expiration_raw)
        parsed_expiration_date = (
            parsed_expiration.date() if isinstance(parsed_expiration, datetime) else parsed_expiration
        )

        if parsed_expiration_date is None:
            logger.info(
                "Compliance expiration evaluation — raw_expiration=%s parsed_date=%s evaluated_at_et=%s comparison_result=%s",
                str(expiration_raw or "").strip() or "(blank)",
                None,
                _current_et_timestamp_str(),
                "skipped_unparseable",
            )
            continue

        comparison_result = "expired" if parsed_expiration_date < today else "not_expired"
        logger.info(
            "Compliance expiration evaluation — raw_expiration=%s parsed_date=%s parsed_expiration_date=%s today=%s evaluated_at_et=%s comparison_result=%s",
            str(expiration_raw or "").strip() or "(blank)",
            parsed_expiration_date.isoformat(),
            parsed_expiration_date.isoformat(),
            today.isoformat(),
            _current_et_timestamp_str(),
            comparison_result,
        )

        if parsed_expiration_date < today:
            expired.append(
                {
                    "required_policy_type": normalized_type,
                    "policy_id": row.get("policy_id") or row.get("id"),
                    "policy_number": row.get("policy_number") or row.get("Policy Number"),
                    "expiration_date_raw": str(expiration_raw or "").strip() or None,
                    "parsed_expiration_date": parsed_expiration_date.isoformat() if parsed_expiration_date else None,
                    "expired_by": "date",
                }
            )

    return expired


def _extract_below_minimum_required_policies(
    raw_data: Dict[str, Any],
    required_policy_requirements: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    below_minimum: List[Dict[str, Any]] = []

    policy_rows = raw_data.get("policies")
    if not isinstance(policy_rows, list):
        return below_minimum

    for row in policy_rows:
        if not isinstance(row, dict):
            continue

        normalized_type = _normalize_policy_type(
            row.get("policy_type") or row.get("Policy Type") or row.get("type")
        )
        if not normalized_type:
            continue

        requirement = required_policy_requirements.get(normalized_type)
        if not requirement:
            continue

        required_minimum = requirement.get("minimum_limit_value")
        if required_minimum is None:
            continue

        required_limit_type = str(requirement.get("required_limit_type") or "any")

        coverage_limits_raw = (
            row.get("coverage_limits")
            or row.get("Coverage Limits")
            or row.get("coverage_limit")
            or row.get("limit")
            or ""
        )
        parsed_policy_limits = _extract_numeric_values(coverage_limits_raw)
        if not parsed_policy_limits:
            continue

        structured_policy_limits = _extract_structured_policy_limits(coverage_limits_raw)

        matched_limit_type = "any"
        matched_limit_value: Optional[float] = None
        if required_limit_type != "any":
            matched_limit_value = structured_policy_limits.get(required_limit_type)
            matched_limit_type = required_limit_type
        else:
            if structured_policy_limits:
                matched_limit_value = max(structured_policy_limits.values())
            else:
                matched_limit_value = max(parsed_policy_limits)

        logger.info(
            "Compliance evaluator parsed policy limits — policy_id=%s policy_type=%s coverage_limits_raw=%s parsed_limits_map=%s required_limit_type=%s matched_limit_type=%s matched_limit_value=%s",
            row.get("policy_id") or row.get("id"),
            normalized_type,
            str(coverage_limits_raw or "").strip() or None,
            structured_policy_limits,
            required_limit_type,
            matched_limit_type,
            matched_limit_value,
        )

        # If requirement specifies a concrete limit type but we cannot parse that bucket,
        # do not fail this rule.
        if required_limit_type != "any" and matched_limit_value is None:
            continue

        best_available_limit = float(matched_limit_value)
        if best_available_limit < float(required_minimum):
            below_minimum.append(
                {
                    "required_policy_type": normalized_type,
                    "policy_id": row.get("policy_id") or row.get("id"),
                    "policy_number": row.get("policy_number") or row.get("Policy Number"),
                    "coverage_limits_raw": str(coverage_limits_raw or "").strip() or None,
                    "parsed_policy_limits": parsed_policy_limits,
                    "parsed_limits_map": structured_policy_limits,
                    "best_available_limit": best_available_limit,
                    "matched_limit_type": matched_limit_type,
                    "matched_limit_value": matched_limit_value,
                    "required_limit_type": required_limit_type,
                    "required_minimum_limit_raw": requirement.get("minimum_limit_raw"),
                    "required_minimum_limit_value": float(required_minimum),
                }
            )

    return below_minimum


def evaluate_returned_coi_compliance(
    payload: ReturnedCoiComplianceInput,
) -> ReturnedCoiComplianceResult:
    """Evaluate returned COI compliance (interface-only stub).

    Current behavior:
    - Returns a deterministic "Needs Review" placeholder result.
    - Emits one structured failure reason so callers can start integrating with
      downstream status/audit/queue code paths safely.

    Future behavior:
    - Compare vendor policy data against client requirements.
    - Produce "Compliant" / "Non-Compliant" / "Needs Review" with populated
      reason codes and queue payload when applicable.
    """

    required_policy_requirements, requirement_source = _extract_required_policy_requirements(payload.raw_data)
    required_policy_types = set(required_policy_requirements.keys())
    processed_policy_types = _extract_processed_policy_types(payload.raw_data)
    policy_rows = payload.raw_data.get("policies")

    processed_policy_snapshot: List[Dict[str, Any]] = []
    if isinstance(policy_rows, list):
        for row in policy_rows:
            if not isinstance(row, dict):
                continue
            processed_policy_snapshot.append(
                {
                    "policy_id": row.get("policy_id") or row.get("id"),
                    "policy_type": _normalize_policy_type(
                        row.get("policy_type") or row.get("Policy Type") or row.get("type")
                    ),
                    "policy_number": row.get("policy_number") or row.get("Policy Number"),
                    "status": row.get("status") or row.get("policy_status") or row.get("Status"),
                    "effective_date": row.get("effective_date") or row.get("Effective Date"),
                    "expiration_date": row.get("expiration_date") or row.get("Expiration Date"),
                    "coverage_limits": row.get("coverage_limits") or row.get("Coverage Limits"),
                }
            )

    logger.info(
        "Compliance evaluator v1 input snapshot — extraction_id=%s certificate_id=%s request_id=%s client_id=%s policy_ids_count=%d required_policy_types=%s requirement_source=%s processed_policy_types=%s processed_policy_snapshot=%s",
        payload.extraction_id,
        payload.certificate_id,
        payload.request_id,
        payload.client_id,
        len(payload.policy_ids or []),
        sorted(required_policy_types),
        requirement_source,
        sorted(processed_policy_types),
        processed_policy_snapshot,
    )

    missing_required_policy_types = sorted(required_policy_types - processed_policy_types)
    expired_required_policies = _extract_expired_required_policies(
        payload.raw_data,
        required_policy_types,
    )
    below_minimum_required_policies = _extract_below_minimum_required_policies(
        payload.raw_data,
        required_policy_requirements,
    )

    if expired_required_policies:
        logger.warning(
            "Compliance evaluator v1 detected expired required policy rows — extraction_id=%s certificate_id=%s details=%s",
            payload.extraction_id,
            payload.certificate_id,
            expired_required_policies,
        )

    if below_minimum_required_policies:
        logger.warning(
            "Compliance evaluator v1 detected below-minimum required policy limits — extraction_id=%s certificate_id=%s details=%s",
            payload.extraction_id,
            payload.certificate_id,
            below_minimum_required_policies,
        )

    failure_reasons: List[ComplianceFailureReason] = []

    if missing_required_policy_types:
        logger.warning(
            "Compliance evaluator v1 detected missing required policy types — extraction_id=%s certificate_id=%s missing=%s",
            payload.extraction_id,
            payload.certificate_id,
            missing_required_policy_types,
        )
        failure_reasons.extend(
            [
                ComplianceFailureReason(
                    code="MISSING_REQUIRED_POLICY_TYPE",
                    message=(
                        "Required policy type not present in processed COI policy set: "
                        f"{policy_type}"
                    ),
                    severity="error",
                    field="Policy Type",
                    metadata={
                        "required_policy_type": policy_type,
                        "requirement_source": requirement_source,
                        "required_policy_types": sorted(required_policy_types),
                        "processed_policy_types": sorted(processed_policy_types),
                        "policy_ids": payload.policy_ids,
                        "request_id": payload.request_id,
                        "client_id": payload.client_id,
                    },
                )
                for policy_type in missing_required_policy_types
            ]
        )

    if expired_required_policies:
        failure_reasons.extend(
            [
                ComplianceFailureReason(
                    code="REQUIRED_POLICY_EXPIRED",
                    message=(
                        "Required policy type is present in processed COI but expired: "
                        f"{item['required_policy_type']}"
                    ),
                    severity="error",
                    field="Expiration Date",
                    policy_id=item.get("policy_id"),
                    metadata={
                        "required_policy_type": item.get("required_policy_type"),
                        "policy_number": item.get("policy_number"),
                        "expiration_date_raw": item.get("expiration_date_raw"),
                        "parsed_expiration_date": item.get("parsed_expiration_date"),
                        "expired_by": item.get("expired_by"),
                        "required_policy_types": sorted(required_policy_types),
                        "processed_policy_types": sorted(processed_policy_types),
                        "policy_ids": payload.policy_ids,
                        "request_id": payload.request_id,
                        "client_id": payload.client_id,
                    },
                )
                for item in expired_required_policies
            ]
        )

    if below_minimum_required_policies:
        failure_reasons.extend(
            [
                ComplianceFailureReason(
                    code="REQUIRED_POLICY_LIMIT_BELOW_MINIMUM",
                    message=(
                        "Required policy type is present in processed COI but below required minimum limit: "
                        f"{item['required_policy_type']}"
                    ),
                    severity="error",
                    field="Coverage Limits",
                    policy_id=item.get("policy_id"),
                    metadata={
                        "required_policy_type": item.get("required_policy_type"),
                        "policy_number": item.get("policy_number"),
                        "coverage_limits_raw": item.get("coverage_limits_raw"),
                        "parsed_policy_limits": item.get("parsed_policy_limits"),
                        "parsed_limits_map": item.get("parsed_limits_map"),
                        "best_available_limit": item.get("best_available_limit"),
                        "required_limit_type": item.get("required_limit_type"),
                        "matched_limit_type": item.get("matched_limit_type"),
                        "matched_limit_value": item.get("matched_limit_value"),
                        "required_minimum_limit_raw": item.get("required_minimum_limit_raw"),
                        "required_minimum_limit_value": item.get("required_minimum_limit_value"),
                        "required_policy_types": sorted(required_policy_types),
                        "processed_policy_types": sorted(processed_policy_types),
                        "policy_ids": payload.policy_ids,
                        "request_id": payload.request_id,
                        "client_id": payload.client_id,
                    },
                )
                for item in below_minimum_required_policies
            ]
        )

    if failure_reasons:
        expired_required_policy_types = sorted(
            {item["required_policy_type"] for item in expired_required_policies if item.get("required_policy_type")}
        )
        below_minimum_policy_types = sorted(
            {
                item["required_policy_type"]
                for item in below_minimum_required_policies
                if item.get("required_policy_type")
            }
        )

        decision_parts: List[str] = []
        if missing_required_policy_types:
            decision_parts.append(
                "Missing required policy types: "
                + ", ".join(missing_required_policy_types)
            )
        if expired_required_policy_types:
            decision_parts.append(
                "Expired required policies: "
                + ", ".join(expired_required_policy_types)
            )
        if below_minimum_policy_types:
            decision_parts.append(
                "Required policies below minimum limit: "
                + ", ".join(below_minimum_policy_types)
            )

        return ReturnedCoiComplianceResult(
            outcome="Non-Compliant",
            decision_summary="; ".join(decision_parts),
            failure_reasons=failure_reasons,
            should_queue_deficiency_email=False,
            deficiency_email_payload=None,
            metadata={
                "stub": False,
                "rule_version": "v1-missing-required-policy-type+required-policy-expired+required-policy-limit-below-minimum",
                "requirement_source": requirement_source,
                "required_policy_types": sorted(required_policy_types),
                "processed_policy_types": sorted(processed_policy_types),
                "missing_required_policy_types": missing_required_policy_types,
                "expired_required_policy_types": expired_required_policy_types,
                "expired_required_policies": expired_required_policies,
                "below_minimum_required_policy_types": below_minimum_policy_types,
                "below_minimum_required_policies": below_minimum_required_policies,
                "required_policy_requirements": required_policy_requirements,
            },
        )

    return ReturnedCoiComplianceResult(
        outcome="Needs Review",
        decision_summary="No deterministic compliance failures detected by current evaluator rules.",
        failure_reasons=[],
        should_queue_deficiency_email=False,
        deficiency_email_payload=None,
        metadata={
            "stub": False,
            "rule_version": "v1-missing-required-policy-type+required-policy-expired+required-policy-limit-below-minimum",
            "requirement_source": requirement_source,
            "required_policy_types": sorted(required_policy_types),
            "required_policy_requirements": required_policy_requirements,
            "processed_policy_types": sorted(processed_policy_types),
            "missing_required_policy_types": missing_required_policy_types,
            "expired_required_policies": [],
            "below_minimum_required_policies": [],
        },
    )
