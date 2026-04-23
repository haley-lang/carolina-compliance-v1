"""Incoming Extractions matcher — vendor/client/request resolution.

Resolution order:
1. Policy number exact match
2. Named insured exact match (Vendor Name)
3. Named insured alias match (Aliases field)
4. Named insured DBA match (DBA Names field)
5. Fuzzy name match (rapidfuzz against names + aliases + DBAs)
6. Broker email match (sender email → Vendor.Broker Email)
7. Unmatched → flag for manual review
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set


MATCH_STATUS_MATCHED = "Matched"
MATCH_STATUS_PENDING = "Pending Match"
MATCH_STATUS_UNMATCHED = "Unmatched"
MATCH_STATUS_ERROR = "Error"

REASON_LOW_CONFIDENCE_EXTRACTION = "LOW_CONFIDENCE_EXTRACTION"
REASON_MISSING_REQUIRED_FIELDS = "MISSING_REQUIRED_FIELDS"
REASON_NO_CANDIDATE_FOUND = "NO_CANDIDATE_FOUND"
REASON_AMBIGUOUS_VENDOR = "AMBIGUOUS_VENDOR"
REASON_AMBIGUOUS_CLIENT = "AMBIGUOUS_CLIENT"
REASON_AMBIGUOUS_REQUEST_CONTEXT = "AMBIGUOUS_REQUEST_CONTEXT"
REASON_CONFLICTING_SIGNALS = "CONFLICTING_SIGNALS"
REASON_INVALID_REFERENCED_ID = "INVALID_REFERENCED_ID"
REASON_FUZZY_REVIEW = "FUZZY_MATCH_NEEDS_REVIEW"

# ── Airtable field IDs for new match audit fields ────────────────────────────
FLD_MATCH_METHOD     = "fldhAbg7pWH79R3I3"   # Incoming Extractions: Match Method
FLD_MATCH_CONFIDENCE = "fldp9gN5g1LFi7ZNA"   # Incoming Extractions: Match Confidence
FLD_PROCESSING_STATUS = "fld4KqSQEX32Zenut"  # Incoming Extractions: Processing Status
FLD_NAMED_INSURED    = "fld4X90MLBQIqTNTn"   # Incoming Extractions: Named Insured
FLD_MATCH_STATUS     = "fldLuIpTPVQvyLlqT"   # Incoming Extractions: Match Status
FLD_MATCHED_VENDOR   = "fldOku9CPphnETmTA"   # Incoming Extractions: Matched Vendor

# Vendor table field IDs
FLD_V_DBA_NAMES      = "fldOsG2aLEFIG4oAC"   # Vendors: DBA Names
FLD_V_BROKER_EMAIL   = "fldcvRY2tcEclVdWc"   # Vendors: Broker Email
FLD_V_ALIASES        = "fldq07F1pGZjzefBk"   # Vendors: Aliases
FLD_V_VENDOR_NAME    = "fldb0BUb3wggDMJMp"   # Vendors: Vendor Name

ALLOWED_INCOMING_EXTRACTIONS_FIELDS: Set[str] = {
    "Match Status",
    "Match Reason Code",
    "Match Decision Summary",
    "Matched Vendor",
    "Matched Client",
    "Matched Client Request",
    FLD_MATCH_METHOD,
    FLD_MATCH_CONFIDENCE,
}

RULE_V1_POLICY_NUMBER_VENDOR_EXACT = "V1_POLICY_NUMBER_VENDOR_EXACT"
RULE_V2_NAMED_INSURED_VENDOR_EXACT = "V2_NAMED_INSURED_VENDOR_EXACT"
RULE_V3_DBA_NAME_MATCH = "V3_DBA_NAME_MATCH"
RULE_V4_FUZZY_NAME_MATCH = "V4_FUZZY_NAME_MATCH"
RULE_V5_BROKER_EMAIL_MATCH = "V5_BROKER_EMAIL_MATCH"
RULE_C1_CERTIFICATE_HOLDER_CLIENT_EXACT = "C1_CERTIFICATE_HOLDER_CLIENT_EXACT"
RULE_R1_SINGLE_OPEN_REQUEST = "R1_SINGLE_OPEN_REQUEST"

HIGH_CONFIDENCE_THRESHOLD = 0.90

# Fuzzy match thresholds
FUZZY_AUTO_MATCH_THRESHOLD = 85
FUZZY_REVIEW_THRESHOLD = 60

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MatchEvaluation:
    match_status: str
    match_reason_code: Optional[str]
    matched_vendor_id: Optional[str]
    matched_client_id: Optional[str]
    matched_request_id: Optional[str]
    applied_rule_id: Optional[str]
    match_method: str = "unmatched"
    match_confidence: int = 0


# US state abbreviations for address line detection
_US_STATES = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia",
    "ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt",
    "va","wa","wv","wi","wy","dc",
}
_STREET_WORDS = {
    "st","street","ave","avenue","blvd","boulevard","rd","road","dr","drive",
    "ln","lane","ct","court","way","pl","place","pkwy","parkway","hwy","highway",
    "suite","ste","apt","unit","floor","fl","po box","p.o. box",
}


def _looks_like_address_line(line: str) -> bool:
    """Return True if a line looks like a street address, city/state, or zip."""
    stripped = line.strip().lower()
    if not stripped:
        return True  # blank lines between name and address
    # Zip code pattern (5 digits or 5+4)
    if re.search(r"\b\d{5}(?:-\d{4})?\b", stripped):
        return True
    # City, STATE pattern
    words = stripped.replace(",", " ").split()
    if len(words) >= 2 and words[-1] in _US_STATES:
        return True
    # Street address (starts with number + street word)
    if re.match(r"^\d+\s", stripped):
        for sw in _STREET_WORDS:
            if sw in stripped:
                return True
    # Lines starting with suite/apt/unit/floor + number
    if re.match(r"^(?:suite|ste|apt|unit|floor|fl)\s+\d", stripped):
        return True
    # PO Box
    if "po box" in stripped or "p.o. box" in stripped:
        return True
    return False


def clean_named_insured(raw: Any) -> str:
    """Strip address lines from a named insured value.

    The extractor sometimes returns "Company Name\\n123 Main St\\nCity, ST 12345".
    This function returns only the company name (first non-address line).
    """
    text = str(raw or "").strip()
    if not text:
        return ""
    lines = text.split("\n")
    # Take lines from the top until we hit something that looks like an address
    name_parts = []
    for line in lines:
        if _looks_like_address_line(line) and name_parts:
            break
        cleaned = line.strip()
        if cleaned:
            name_parts.append(cleaned)
    return " ".join(name_parts) if name_parts else text


def normalize_match_text(value: Any) -> str:
    """Deterministic normalization for exact matching only."""
    text = str(value or "")
    return " ".join(text.strip().lower().split())


def _normalize_client_match_text(value: Any) -> str:
    """Normalization used only for client name/certificate-holder matching."""
    text = str(value or "").upper().strip()
    if not text:
        return ""
    text = text.replace("&", " AND ")
    text = text.replace("'", "")
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    return " ".join(text.split())


def _normalize_aliases(aliases_raw: Any) -> Set[str]:
    if not isinstance(aliases_raw, str):
        return set()
    return {
        normalize_match_text(part)
        for part in aliases_raw.split(",")
        if normalize_match_text(part)
    }


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_non_partial_exact_value(value: str) -> bool:
    if not value:
        return False
    if "*" in value or "..." in value:
        return False
    if value in {"unknown", "n/a", "na", "none", "null", "tbd"}:
        return False
    if value.endswith("-"):
        return False
    return True


def _parse_linked_ids(value: Any) -> List[str]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str) and item]
    if isinstance(value, str) and value:
        return [value]
    return []


def _extract_policy_number_candidates(extraction_fields: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []

    policy_number_value = extraction_fields.get("Policy Number")
    policy_number_confidence = _as_float(
        extraction_fields.get("Policy Number Confidence")
        or extraction_fields.get("policy_number_confidence")
    )

    if isinstance(policy_number_value, dict):
        nested_value = normalize_match_text(
            policy_number_value.get("value") or policy_number_value.get("policy_number")
        )
        nested_confidence = _as_float(policy_number_value.get("confidence"))
        if (
            nested_value
            and _is_non_partial_exact_value(nested_value)
            and nested_confidence is not None
            and nested_confidence >= HIGH_CONFIDENCE_THRESHOLD
        ):
            candidates.append(nested_value)
    else:
        normalized_policy_number = normalize_match_text(policy_number_value)
        if (
            normalized_policy_number
            and _is_non_partial_exact_value(normalized_policy_number)
            and policy_number_confidence is not None
            and policy_number_confidence >= HIGH_CONFIDENCE_THRESHOLD
        ):
            candidates.append(normalized_policy_number)

    policies_value = extraction_fields.get("Policies")
    if isinstance(policies_value, list):
        for policy_item in policies_value:
            if not isinstance(policy_item, dict):
                continue
            normalized_policy_number = normalize_match_text(
                policy_item.get("policy_number") or policy_item.get("Policy Number")
            )
            confidence = _as_float(
                policy_item.get("confidence")
                or policy_item.get("policy_number_confidence")
                or policy_item.get("Policy Number Confidence")
            )
            if (
                normalized_policy_number
                and _is_non_partial_exact_value(normalized_policy_number)
                and confidence is not None
                and confidence >= HIGH_CONFIDENCE_THRESHOLD
            ):
                candidates.append(normalized_policy_number)

    return list(dict.fromkeys(candidates))


def _resolve_vendor_by_policy_number(
    extraction_fields: Dict[str, Any],
    policy_records: Iterable[Dict[str, Any]],
) -> MatchEvaluation:
    policy_number_candidates = _extract_policy_number_candidates(extraction_fields)
    if not policy_number_candidates:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_NO_CANDIDATE_FOUND,
            matched_vendor_id=None,
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )

    matched_vendor_ids: List[str] = []
    for policy_record in policy_records:
        policy_fields = policy_record.get("fields")
        if not isinstance(policy_fields, dict):
            return MatchEvaluation(
                match_status=MATCH_STATUS_ERROR,
                match_reason_code=REASON_INVALID_REFERENCED_ID,
                matched_vendor_id=None,
                matched_client_id=None,
                matched_request_id=None,
                applied_rule_id=None,
            )

        normalized_policy_number = normalize_match_text(policy_fields.get("Policy Number"))
        if not _is_non_partial_exact_value(normalized_policy_number):
            continue
        if normalized_policy_number not in policy_number_candidates:
            continue

        vendor_ids = _parse_linked_ids(
            policy_fields.get("Vendor")
            or policy_fields.get("Vendor Link")
            or policy_fields.get("Vendors")
        )
        matched_vendor_ids.extend(vendor_ids)

    unique_vendor_ids = list(dict.fromkeys(matched_vendor_ids))
    if len(unique_vendor_ids) == 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=unique_vendor_ids[0],
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=RULE_V1_POLICY_NUMBER_VENDOR_EXACT,
            match_method="exact", match_confidence=100,
        )

    if len(unique_vendor_ids) > 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_AMBIGUOUS_VENDOR,
            matched_vendor_id=None,
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )

    return MatchEvaluation(
        match_status=MATCH_STATUS_PENDING,
        match_reason_code=REASON_NO_CANDIDATE_FOUND,
        matched_vendor_id=None,
        matched_client_id=None,
        matched_request_id=None,
        applied_rule_id=None,
    )


def _resolve_vendor_by_named_insured(
    extraction_fields: Dict[str, Any],
    vendor_records: Iterable[Dict[str, Any]],
) -> MatchEvaluation:
    named_insured = normalize_match_text(extraction_fields.get("Named Insured"))
    if not named_insured or not _is_non_partial_exact_value(named_insured):
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_MISSING_REQUIRED_FIELDS,
            matched_vendor_id=None,
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )

    matched_vendor_ids: List[str] = []
    for vendor in vendor_records:
        vendor_id = vendor.get("id")
        vendor_fields = vendor.get("fields")
        if not vendor_id or not isinstance(vendor_fields, dict):
            return MatchEvaluation(
                match_status=MATCH_STATUS_ERROR,
                match_reason_code=REASON_INVALID_REFERENCED_ID,
                matched_vendor_id=None,
                matched_client_id=None,
                matched_request_id=None,
                applied_rule_id=None,
            )

        vendor_name = normalize_match_text(vendor_fields.get("Vendor Name"))
        aliases = _normalize_aliases(vendor_fields.get("Aliases"))

        if named_insured == vendor_name:
            matched_vendor_ids.append((vendor_id, "exact"))
        elif named_insured in aliases:
            matched_vendor_ids.append((vendor_id, "alias"))

    unique_ids = list(dict.fromkeys(vid for vid, _ in matched_vendor_ids))
    if len(unique_ids) == 1:
        method = matched_vendor_ids[0][1]
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=unique_ids[0],
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=RULE_V2_NAMED_INSURED_VENDOR_EXACT,
            match_method=method, match_confidence=100,
        )

    if len(unique_ids) > 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_AMBIGUOUS_VENDOR,
            matched_vendor_id=None,
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )

    return MatchEvaluation(
        match_status=MATCH_STATUS_PENDING,
        match_reason_code=REASON_NO_CANDIDATE_FOUND,
        matched_vendor_id=None,
        matched_client_id=None,
        matched_request_id=None,
        applied_rule_id=None,
    )


def _normalize_dba_names(dba_raw: Any) -> Set[str]:
    """Parse comma-separated DBA Names field into normalized set."""
    if not isinstance(dba_raw, str):
        return set()
    return {
        normalize_match_text(part)
        for part in dba_raw.split(",")
        if normalize_match_text(part)
    }


def _resolve_vendor_by_dba(
    extraction_fields: Dict[str, Any],
    vendor_records: Iterable[Dict[str, Any]],
) -> MatchEvaluation:
    """Match named insured against DBA Names on vendor records."""
    named_insured = normalize_match_text(extraction_fields.get("Named Insured"))
    if not named_insured or not _is_non_partial_exact_value(named_insured):
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_NO_CANDIDATE_FOUND,
            matched_vendor_id=None, matched_client_id=None,
            matched_request_id=None, applied_rule_id=None,
        )

    matched_ids: List[str] = []
    for vendor in vendor_records:
        vendor_id = vendor.get("id")
        vendor_fields = vendor.get("fields", {})
        dba_names = _normalize_dba_names(
            vendor_fields.get("DBA Names") or vendor_fields.get(FLD_V_DBA_NAMES)
        )
        if named_insured in dba_names:
            matched_ids.append(vendor_id)

    unique = list(dict.fromkeys(matched_ids))
    if len(unique) == 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=unique[0], matched_client_id=None,
            matched_request_id=None, applied_rule_id=RULE_V3_DBA_NAME_MATCH,
            match_method="dba", match_confidence=100,
        )
    if len(unique) > 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_AMBIGUOUS_VENDOR,
            matched_vendor_id=None, matched_client_id=None,
            matched_request_id=None, applied_rule_id=None,
        )
    return MatchEvaluation(
        match_status=MATCH_STATUS_PENDING,
        match_reason_code=REASON_NO_CANDIDATE_FOUND,
        matched_vendor_id=None, matched_client_id=None,
        matched_request_id=None, applied_rule_id=None,
    )


def _resolve_vendor_by_fuzzy(
    extraction_fields: Dict[str, Any],
    vendor_records: Iterable[Dict[str, Any]],
) -> MatchEvaluation:
    """Fuzzy match named insured against vendor names, aliases, and DBA names."""
    try:
        from rapidfuzz import fuzz
    except ImportError:
        logger.warning("rapidfuzz not installed — skipping fuzzy matching")
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_NO_CANDIDATE_FOUND,
            matched_vendor_id=None, matched_client_id=None,
            matched_request_id=None, applied_rule_id=None,
        )

    named_insured = normalize_match_text(extraction_fields.get("Named Insured"))
    if not named_insured or not _is_non_partial_exact_value(named_insured):
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_MISSING_REQUIRED_FIELDS,
            matched_vendor_id=None, matched_client_id=None,
            matched_request_id=None, applied_rule_id=None,
        )

    best_score = 0
    best_vendor_id = None
    best_matched_name = ""

    for vendor in vendor_records:
        vendor_id = vendor.get("id")
        vendor_fields = vendor.get("fields", {})

        # Collect all names to compare against
        candidates: List[str] = []
        vendor_name = normalize_match_text(vendor_fields.get("Vendor Name"))
        if vendor_name:
            candidates.append(vendor_name)
        candidates.extend(_normalize_aliases(
            vendor_fields.get("Aliases") or vendor_fields.get(FLD_V_ALIASES)
        ))
        candidates.extend(_normalize_dba_names(
            vendor_fields.get("DBA Names") or vendor_fields.get(FLD_V_DBA_NAMES)
        ))

        for candidate in candidates:
            score = fuzz.ratio(named_insured, candidate)
            if score > best_score:
                best_score = score
                best_vendor_id = vendor_id
                best_matched_name = candidate

    if best_score >= FUZZY_AUTO_MATCH_THRESHOLD and best_vendor_id:
        logger.info(
            "Fuzzy auto-match: '%s' → '%s' (score=%d, vendor=%s)",
            named_insured, best_matched_name, best_score, best_vendor_id,
        )
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=best_vendor_id, matched_client_id=None,
            matched_request_id=None, applied_rule_id=RULE_V4_FUZZY_NAME_MATCH,
            match_method="fuzzy", match_confidence=int(best_score),
        )

    if best_score >= FUZZY_REVIEW_THRESHOLD and best_vendor_id:
        logger.info(
            "Fuzzy match needs review: '%s' → '%s' (score=%d, vendor=%s)",
            named_insured, best_matched_name, best_score, best_vendor_id,
        )
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_FUZZY_REVIEW,
            matched_vendor_id=best_vendor_id, matched_client_id=None,
            matched_request_id=None, applied_rule_id=RULE_V4_FUZZY_NAME_MATCH,
            match_method="fuzzy", match_confidence=int(best_score),
        )

    return MatchEvaluation(
        match_status=MATCH_STATUS_PENDING,
        match_reason_code=REASON_NO_CANDIDATE_FOUND,
        matched_vendor_id=None, matched_client_id=None,
        matched_request_id=None, applied_rule_id=None,
        match_method="unmatched", match_confidence=int(best_score) if best_score > 0 else 0,
    )


def _resolve_vendor_by_broker_email(
    extraction_fields: Dict[str, Any],
    vendor_records: Iterable[Dict[str, Any]],
) -> MatchEvaluation:
    """Match sender/contact email against Broker Email field on vendor records."""
    # Collect all email addresses from the extraction
    sender_emails: Set[str] = set()
    contact_emails_raw = extraction_fields.get("Contact Emails") or ""
    if isinstance(contact_emails_raw, str):
        for part in re.split(r"[,;\s]+", contact_emails_raw):
            cleaned = part.strip().lower()
            if "@" in cleaned:
                sender_emails.add(cleaned)
    if isinstance(contact_emails_raw, list):
        for email in contact_emails_raw:
            if isinstance(email, str) and "@" in email:
                sender_emails.add(email.strip().lower())

    # Also check Sender Email if available
    sender = extraction_fields.get("Sender Email") or ""
    if isinstance(sender, str) and "@" in sender:
        # Extract email from "Name <email>" format
        email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", sender.lower())
        if email_match:
            sender_emails.add(email_match.group())

    if not sender_emails:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_NO_CANDIDATE_FOUND,
            matched_vendor_id=None, matched_client_id=None,
            matched_request_id=None, applied_rule_id=None,
        )

    matched_ids: List[str] = []
    for vendor in vendor_records:
        vendor_id = vendor.get("id")
        vendor_fields = vendor.get("fields", {})
        broker_email = (
            vendor_fields.get("Broker Email") or vendor_fields.get(FLD_V_BROKER_EMAIL) or ""
        ).strip().lower()
        if broker_email and broker_email in sender_emails:
            matched_ids.append(vendor_id)

    unique = list(dict.fromkeys(matched_ids))
    if len(unique) == 1:
        logger.info("Broker email match: %s → vendor %s", sender_emails, unique[0])
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=unique[0], matched_client_id=None,
            matched_request_id=None, applied_rule_id=RULE_V5_BROKER_EMAIL_MATCH,
            match_method="broker_email", match_confidence=90,
        )
    if len(unique) > 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_AMBIGUOUS_VENDOR,
            matched_vendor_id=None, matched_client_id=None,
            matched_request_id=None, applied_rule_id=None,
        )
    return MatchEvaluation(
        match_status=MATCH_STATUS_PENDING,
        match_reason_code=REASON_NO_CANDIDATE_FOUND,
        matched_vendor_id=None, matched_client_id=None,
        matched_request_id=None, applied_rule_id=None,
    )


def _resolve_client_for_vendor(
    extraction_fields: Dict[str, Any],
    matched_vendor_id: str,
    client_records: Iterable[Dict[str, Any]],
) -> MatchEvaluation:
    raw_certificate_holder = extraction_fields.get("Certificate Holder")
    certificate_holder = _normalize_client_match_text(raw_certificate_holder)
    logger.info(
        "Client resolution certificate holder extracted raw=%r normalized=%r",
        raw_certificate_holder,
        certificate_holder,
    )
    if not certificate_holder or not _is_non_partial_exact_value(certificate_holder):
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=matched_vendor_id,
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )

    matched_client_ids: List[str] = []
    fallback_client_name_ids: List[str] = []
    for client_record in client_records:
        client_id = client_record.get("id")
        client_fields = client_record.get("fields")
        if not client_id or not isinstance(client_fields, dict):
            return MatchEvaluation(
                match_status=MATCH_STATUS_ERROR,
                match_reason_code=REASON_INVALID_REFERENCED_ID,
                matched_vendor_id=matched_vendor_id,
                matched_client_id=None,
                matched_request_id=None,
                applied_rule_id=None,
            )

        client_vendor_ids = _parse_linked_ids(
            client_fields.get("Vendor")
            or client_fields.get("Vendor Link")
            or client_fields.get("Vendors")
        )
        if client_vendor_ids and matched_vendor_id not in client_vendor_ids:
            continue

        client_certificate_holder = _normalize_client_match_text(
            client_fields.get("Certificate Holder")
        )
        if certificate_holder == client_certificate_holder:
            matched_client_ids.append(client_id)

        client_name = _normalize_client_match_text(client_fields.get("Client Name"))
        if certificate_holder == client_name:
            fallback_client_name_ids.append(client_id)

    unique_client_ids = list(dict.fromkeys(matched_client_ids))
    if len(unique_client_ids) == 1:
        logger.info(
            "Resolved client for vendor %s using Clients['Certificate Holder'] value=%r (normalized=%r): client=%s",
            matched_vendor_id,
            raw_certificate_holder,
            certificate_holder,
            unique_client_ids[0],
        )
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=matched_vendor_id,
            matched_client_id=unique_client_ids[0],
            matched_request_id=None,
            applied_rule_id=RULE_C1_CERTIFICATE_HOLDER_CLIENT_EXACT,
        )

    fallback_unique_client_ids = list(dict.fromkeys(fallback_client_name_ids))
    if len(fallback_unique_client_ids) == 1:
        logger.info(
            "Resolved client for vendor %s using Clients['Client Name'] fallback value=%r (normalized=%r): client=%s",
            matched_vendor_id,
            raw_certificate_holder,
            certificate_holder,
            fallback_unique_client_ids[0],
        )
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=matched_vendor_id,
            matched_client_id=fallback_unique_client_ids[0],
            matched_request_id=None,
            applied_rule_id=RULE_C1_CERTIFICATE_HOLDER_CLIENT_EXACT,
        )

    if len(unique_client_ids) > 1 or len(fallback_unique_client_ids) > 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_AMBIGUOUS_CLIENT,
            matched_vendor_id=matched_vendor_id,
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )

    return MatchEvaluation(
        match_status=MATCH_STATUS_MATCHED,
        match_reason_code=None,
        matched_vendor_id=matched_vendor_id,
        matched_client_id=None,
        matched_request_id=None,
        applied_rule_id=None,
    )


def _is_open_request(request_fields: Dict[str, Any]) -> bool:
    if request_fields.get("Open") is True:
        return True
    return (
        normalize_match_text(request_fields.get("Status")) == "open"
        or normalize_match_text(request_fields.get("Request Status")) == "open"
    )


def _resolve_request_for_vendor_and_client(
    matched_vendor_id: str,
    matched_client_id: str,
    request_records: Iterable[Dict[str, Any]],
) -> MatchEvaluation:
    matched_open_request_ids: List[str] = []

    for request_record in request_records:
        request_id = request_record.get("id")
        request_fields = request_record.get("fields")
        if not request_id or not isinstance(request_fields, dict):
            return MatchEvaluation(
                match_status=MATCH_STATUS_ERROR,
                match_reason_code=REASON_INVALID_REFERENCED_ID,
                matched_vendor_id=matched_vendor_id,
                matched_client_id=matched_client_id,
                matched_request_id=None,
                applied_rule_id=None,
            )

        request_vendor_ids = _parse_linked_ids(
            request_fields.get("Vendor")
            or request_fields.get("Vendor Link")
            or request_fields.get("Vendors")
        )
        request_client_ids = _parse_linked_ids(
            request_fields.get("Client")
            or request_fields.get("Client Link")
            or request_fields.get("Clients")
        )
        if matched_vendor_id not in request_vendor_ids:
            continue
        if matched_client_id not in request_client_ids:
            continue
        if not _is_open_request(request_fields):
            continue
        matched_open_request_ids.append(request_id)

    unique_request_ids = list(dict.fromkeys(matched_open_request_ids))
    if len(unique_request_ids) == 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id=matched_vendor_id,
            matched_client_id=matched_client_id,
            matched_request_id=unique_request_ids[0],
            applied_rule_id=RULE_R1_SINGLE_OPEN_REQUEST,
        )

    if len(unique_request_ids) > 1:
        return MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_AMBIGUOUS_REQUEST_CONTEXT,
            matched_vendor_id=matched_vendor_id,
            matched_client_id=matched_client_id,
            matched_request_id=None,
            applied_rule_id=None,
        )

    return MatchEvaluation(
        match_status=MATCH_STATUS_MATCHED,
        match_reason_code=None,
        matched_vendor_id=matched_vendor_id,
        matched_client_id=matched_client_id,
        matched_request_id=None,
        applied_rule_id=None,
    )


def evaluate_vendor_match(
    extraction_fields: Dict[str, Any],
    vendor_records: Iterable[Dict[str, Any]],
    policy_records: Optional[Iterable[Dict[str, Any]]] = None,
    client_records: Optional[Iterable[Dict[str, Any]]] = None,
    request_records: Optional[Iterable[Dict[str, Any]]] = None,
) -> MatchEvaluation:
    """Pure evaluator: no side effects, no external calls.

    Resolution order:
    1. Policy number exact → 2. Named insured exact/alias →
    3. DBA name → 4. Fuzzy name → 5. Broker email → 6. Unmatched
    """
    try:
        vendor_list = list(vendor_records)  # materialize so we can iterate multiple times

        # Clean named insured — strip address lines if contaminated
        raw_insured = extraction_fields.get("Named Insured")
        if raw_insured and "\n" in str(raw_insured):
            cleaned = clean_named_insured(raw_insured)
            if cleaned != str(raw_insured).strip():
                logger.info("Cleaned named insured: %r → %r", str(raw_insured)[:60], cleaned)
                extraction_fields = dict(extraction_fields)
                extraction_fields["Named Insured"] = cleaned

        # ── Step 1: Policy number exact match ────────────────────────────────
        vendor_eval = MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_NO_CANDIDATE_FOUND,
            matched_vendor_id=None, matched_client_id=None,
            matched_request_id=None, applied_rule_id=None,
        )

        if policy_records is not None:
            vendor_eval = _resolve_vendor_by_policy_number(extraction_fields, policy_records)
            if vendor_eval.match_status == MATCH_STATUS_MATCHED and vendor_eval.matched_vendor_id:
                pass
            elif vendor_eval.match_reason_code == REASON_AMBIGUOUS_VENDOR:
                return vendor_eval
            else:
                # ── Step 2: Named insured exact / alias match ────────────────
                vendor_eval = _resolve_vendor_by_named_insured(extraction_fields, vendor_list)
        else:
            vendor_eval = _resolve_vendor_by_named_insured(extraction_fields, vendor_list)

        # ── Step 3: DBA name match ───────────────────────────────────────────
        if vendor_eval.match_status != MATCH_STATUS_MATCHED or not vendor_eval.matched_vendor_id:
            dba_eval = _resolve_vendor_by_dba(extraction_fields, vendor_list)
            if dba_eval.match_status == MATCH_STATUS_MATCHED:
                vendor_eval = dba_eval
            elif dba_eval.match_reason_code == REASON_AMBIGUOUS_VENDOR:
                return dba_eval

        # ── Step 4: Fuzzy name match ─────────────────────────────────────────
        if vendor_eval.match_status != MATCH_STATUS_MATCHED or not vendor_eval.matched_vendor_id:
            fuzzy_eval = _resolve_vendor_by_fuzzy(extraction_fields, vendor_list)
            if fuzzy_eval.match_status == MATCH_STATUS_MATCHED:
                vendor_eval = fuzzy_eval
            elif fuzzy_eval.match_reason_code == REASON_FUZZY_REVIEW:
                # Fuzzy match below auto-threshold but above review threshold —
                # return as Pending Match with the candidate vendor for human review
                return fuzzy_eval

        # ── Step 5: Broker email match ───────────────────────────────────────
        if vendor_eval.match_status != MATCH_STATUS_MATCHED or not vendor_eval.matched_vendor_id:
            broker_eval = _resolve_vendor_by_broker_email(extraction_fields, vendor_list)
            if broker_eval.match_status == MATCH_STATUS_MATCHED:
                vendor_eval = broker_eval

        # ── Step 6: Unmatched ────────────────────────────────────────────────
        if vendor_eval.match_status != MATCH_STATUS_MATCHED or not vendor_eval.matched_vendor_id:
            return MatchEvaluation(
                match_status=MATCH_STATUS_UNMATCHED,
                match_reason_code=REASON_NO_CANDIDATE_FOUND,
                matched_vendor_id=None, matched_client_id=None,
                matched_request_id=None, applied_rule_id=None,
                match_method="unmatched", match_confidence=0,
            )

        final_eval = vendor_eval

        if client_records is not None:
            logger.info("Vendor matched → proceeding to client resolution")
            client_eval = _resolve_client_for_vendor(
                extraction_fields,
                vendor_eval.matched_vendor_id,
                client_records,
            )
            if client_eval.match_status == MATCH_STATUS_ERROR:
                return client_eval
            if client_eval.match_status == MATCH_STATUS_PENDING:
                return client_eval
            if client_eval.matched_client_id:
                final_eval = MatchEvaluation(
                    match_status=MATCH_STATUS_MATCHED,
                    match_reason_code=None,
                    matched_vendor_id=client_eval.matched_vendor_id,
                    matched_client_id=client_eval.matched_client_id,
                    matched_request_id=None,
                    applied_rule_id=RULE_C1_CERTIFICATE_HOLDER_CLIENT_EXACT,
                    match_method=vendor_eval.match_method,
                    match_confidence=vendor_eval.match_confidence,
                )

        if (
            request_records is not None
            and final_eval.matched_vendor_id
            and final_eval.matched_client_id
        ):
            request_eval = _resolve_request_for_vendor_and_client(
                final_eval.matched_vendor_id,
                final_eval.matched_client_id,
                request_records,
            )
            if request_eval.match_status == MATCH_STATUS_ERROR:
                return request_eval
            if request_eval.match_status == MATCH_STATUS_PENDING:
                return request_eval
            if request_eval.matched_request_id:
                return MatchEvaluation(
                    match_status=MATCH_STATUS_MATCHED,
                    match_reason_code=None,
                    matched_vendor_id=request_eval.matched_vendor_id,
                    matched_client_id=request_eval.matched_client_id,
                    matched_request_id=request_eval.matched_request_id,
                    applied_rule_id=RULE_R1_SINGLE_OPEN_REQUEST,
                )

        return final_eval
    except Exception:
        return MatchEvaluation(
            match_status=MATCH_STATUS_ERROR,
            match_reason_code=REASON_INVALID_REFERENCED_ID,
            matched_vendor_id=None,
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )


def _reason_code_equivalent(a: Any, b: Any) -> bool:
    empty_a = a is None or a == ""
    empty_b = b is None or b == ""
    if empty_a and empty_b:
        return True
    return a == b


def _linked_record_value(record_id: Optional[str]) -> List[str]:
    return [record_id] if record_id else []


def _summary_value(value: Optional[str]) -> str:
    return value or ""


def build_match_decision_summary(evaluation: MatchEvaluation) -> str:
    return (
        f"status={_summary_value(evaluation.match_status)}; "
        f"reason={_summary_value(evaluation.match_reason_code)}; "
        f"vendor={_summary_value(evaluation.matched_vendor_id)}; "
        f"client={_summary_value(evaluation.matched_client_id)}; "
        f"request={_summary_value(evaluation.matched_request_id)}; "
        f"rule={_summary_value(evaluation.applied_rule_id)}; "
        f"method={evaluation.match_method}; "
        f"confidence={evaluation.match_confidence}"
    )


def build_incoming_extraction_patch(
    existing_fields: Dict[str, Any],
    evaluation: MatchEvaluation,
) -> Dict[str, Any]:
    """Build a constrained patch for Incoming Extractions only."""
    decision_summary = build_match_decision_summary(evaluation)
    target_values: Dict[str, Any] = {
        "Match Status": evaluation.match_status,
        "Match Reason Code": evaluation.match_reason_code,
        "Match Decision Summary": decision_summary,
        "Matched Vendor": _linked_record_value(evaluation.matched_vendor_id),
        "Matched Client": _linked_record_value(evaluation.matched_client_id),
        "Matched Client Request": _linked_record_value(evaluation.matched_request_id),
        FLD_MATCH_METHOD: evaluation.match_method,
        FLD_MATCH_CONFIDENCE: evaluation.match_confidence,
    }

    patch: Dict[str, Any] = {}
    for field, new_value in target_values.items():
        current_value = existing_fields.get(field)

        if field == "Match Reason Code":
            if not _reason_code_equivalent(current_value, new_value):
                patch[field] = new_value
            continue

        if current_value != new_value:
            patch[field] = new_value

    unexpected_fields = set(patch.keys()) - ALLOWED_INCOMING_EXTRACTIONS_FIELDS
    if unexpected_fields:
        raise ValueError(f"Patch contains disallowed fields: {sorted(unexpected_fields)}")

    return patch


def apply_incoming_extraction_match_update(
    incoming_extractions_table: Any,
    record_id: str,
    existing_fields: Dict[str, Any],
    evaluation: MatchEvaluation,
) -> bool:
    """Apply idempotent update to one Incoming Extractions record.

    Returns True if an update was written, False if no-op.
    """
    patch = build_incoming_extraction_patch(existing_fields, evaluation)
    if not patch:
        return False

    incoming_extractions_table.update(record_id, patch, typecast=True)
    return True
