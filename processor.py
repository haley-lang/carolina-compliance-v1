"""
Module 4 — COI Processor
Reads the newest "Imported" record from Airtable "Incoming Extractions",
matches the vendor, creates Insurance Policy and Certificate records,
and updates the extraction status.
"""

import json
import logging
import re
from urllib.parse import urlencode
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from pyairtable import Api
import pytz
import requests

from incoming_extraction_matcher_slice import (
    apply_incoming_extraction_match_update,
    evaluate_vendor_match,
)
from returned_coi_compliance_evaluator import (
    ReturnedCoiComplianceInput,
    evaluate_returned_coi_compliance,
)

# Load .env using an absolute path before importing config
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config  # noqa: E402 — must come after load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

eastern = pytz.timezone("America/New_York")


COMPLIANCE_CERT_FIELD_NAMES = {
    "status": "Compliance Status",
    "evaluated_at": "Compliance Evaluated At",
    "decision_summary": "Compliance Decision Summary",
    "failure_reasons_json": "Compliance Failure Reasons",
}

# ── Table names ───────────────────────────────────────────────────────────────
TABLE_INCOMING   = "Incoming Extractions"
TABLE_VENDORS    = "Vendors"
TABLE_POLICIES   = "tblpPcmm5ANE0bMNB"
TABLE_CLIENTS    = "Clients"
TABLE_REQUESTS   = "COI Requests"
TABLE_REQUIREMENTS = "Client Requirements"
TABLE_CERTS      = "Insurance Certificates"
TABLE_EMAIL_QUEUE = "Email Queue"
TABLE_TEMPLATES  = "Templates"


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_base_id(raw: str) -> str:
    """Extract just the appXXXXXXXXXX portion from AIRTABLE_BASE_ID."""
    match = re.search(r"(app[A-Za-z0-9]{10,})", raw)
    if not match:
        raise ValueError(
            f"AIRTABLE_BASE_ID does not look like a valid base ID: '{raw}'. "
            "It should start with 'app' (e.g. appCGgww0Pt7KE04u)."
        )
    return match.group(1)


def get_tables(api: Api, base_id: str) -> dict:
    """Return a dict of named pyairtable Table objects."""
    return {
        name: api.table(base_id, name)
        for name in (
            TABLE_INCOMING,
            TABLE_VENDORS,
            TABLE_POLICIES,
            TABLE_CLIENTS,
            TABLE_REQUESTS,
            TABLE_REQUIREMENTS,
            TABLE_CERTS,
            TABLE_EMAIL_QUEUE,
            TABLE_TEMPLATES,
        )
    }


def _escape_formula_value(value: str) -> str:
    return str(value or "").replace("\\", "\\\\").replace("'", "\\'")


def _extract_vendor_email(vendor_fields: dict) -> str:
    for candidate in ("Vendor Email", "Email", "Primary Email", "Contact Email"):
        value = vendor_fields.get(candidate)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _resolve_client_name(matched_client_id: Optional[str], client_records: list) -> str:
    if matched_client_id:
        matched_client = next(
            (record for record in client_records if record.get("id") == matched_client_id),
            None,
        )
        if matched_client:
            fields = matched_client.get("fields", {})
            for candidate in ("Client Name", "Name"):
                value = fields.get(candidate)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return "[Client Name]"


def _build_deficiency_email_subject(client_name: str) -> str:
    return f"Action Needed: Updated COI Required for {client_name}"


def _build_deficiency_reason_lines(failure_reasons: list) -> list[str]:
    lines: list[str] = []
    for reason in failure_reasons or []:
        code = getattr(reason, "code", None)
        metadata = getattr(reason, "metadata", {}) or {}
        policy_type = (
            metadata.get("required_policy_type")
            or metadata.get("policy_type")
            or "Required policy"
        )

        if code == "MISSING_REQUIRED_POLICY_TYPE":
            lines.append(f"- {policy_type} coverage is missing")
        elif code == "REQUIRED_POLICY_EXPIRED":
            lines.append(f"- {policy_type} policy has expired and must be current")
        elif code == "REQUIRED_POLICY_LIMIT_BELOW_MINIMUM":
            lines.append(f"- {policy_type} coverage does not meet the required limit")
        else:
            lines.append("- Please review policy requirement details and provide needed corrections")
    return lines


def _build_deficiency_email_body(
    vendor_name: str,
    client_name: str,
    failure_reasons: list,
    certificate_id: str,
    source_filename: str,
) -> str:
    safe_vendor_name = vendor_name or "Vendor"
    reason_lines = _build_deficiency_reason_lines(failure_reasons)
    reasons_block = "\n".join(reason_lines) if reason_lines else "- Please review all policy requirement deficiencies."

    return (
        f"Hi {safe_vendor_name},\n\n"
        f"Your Certificate of Insurance for {client_name} has been reviewed and is currently not compliant with requirements.\n\n"
        "To proceed, please provide an updated COI addressing the following:\n\n"
        f"{reasons_block}\n\n"
        f"Please also ensure that {client_name} is listed as the certificate holder on the updated certificate.\n\n"
        "You may reply directly to this email with the updated COI.\n\n"
        "If you have any questions or need assistance, we’re happy to help.\n\n"
        "Best regards,  \n"
        "Carolina Compliance Solutions"
    )


def _fetch_deficiency_template_record(templates_table) -> Optional[dict]:
    try:
        records = templates_table.all(
            formula="{Type}='Deficiency'",
            max_records=1,
        )
        if records:
            return records[0]["fields"]
        return None
    except Exception as e:
        logging.error(f"Deficiency template lookup failed — error={e}")
        return None


def _apply_deficiency_template_placeholders(
    template_text: str,
    vendor_name: str,
    client_name: str,
    compliance_failure_reasons_text: str,
) -> str:
    return (
        str(template_text or "")
        .replace("{{Vendor Name}}", vendor_name or "Vendor")
        .replace("{{Client Name}}", client_name or "[Client Name]")
        .replace(
            "{{Compliance Failure Reasons}}",
            compliance_failure_reasons_text or "- Please review all policy requirement deficiencies.",
        )
    )


def _find_existing_active_unsent_deficiency_queue_record(
    email_queue_table,
    vendor_id: str,
    certificate_id: str,
    source_filename: str,
) -> Optional[str]:
    escaped_vendor_id = _escape_formula_value(vendor_id)
    formula = (
        "AND("
        "{Email Type}='Deficiency Request',"
        "{Record Status}='Active',"
        "NOT({Email Status}='Sent'),"
        f"FIND('{escaped_vendor_id}', ARRAYJOIN({{Vendor}}))"
        ")"
    )

    records = email_queue_table.all(formula=formula)
    for record in records:
        fields = record.get("fields", {})
        subject = str(fields.get("Subject") or "")
        body = str(fields.get("Body") or "")
        text = f"{subject}\n{body}"

        certificate_match = certificate_id and certificate_id in text
        source_match = bool(source_filename and source_filename in text)

        if certificate_match or source_match:
            return record.get("id")

    return None


def queue_deficiency_email_if_needed(
    email_queue_table,
    templates_table,
    certs_table,
    vendor_id: str,
    vendor_fields: dict,
    vendor_name: str,
    matched_client_id: Optional[str],
    client_records: list,
    certificate_id: str,
    source_filename: str,
    failure_reasons: list,
) -> None:
    vendor_email = _extract_vendor_email(vendor_fields)
    if not vendor_email:
        logger.warning(
            "Deficiency queue skipped: missing vendor email — vendor_id=%s vendor_name=%s",
            vendor_id,
            vendor_name,
        )
        return

    client_name = _resolve_client_name(matched_client_id, client_records)
    if client_name == "[Client Name]":
        logger.info(
            "Deficiency queue client name fallback used — vendor_id=%s vendor_name=%s",
            vendor_id,
            vendor_name,
        )
    else:
        logger.info(
            "Deficiency queue client name resolved — vendor_id=%s client_name=%s",
            vendor_id,
            client_name,
        )

    try:
        existing_id = _find_existing_active_unsent_deficiency_queue_record(
            email_queue_table=email_queue_table,
            vendor_id=vendor_id,
            certificate_id=certificate_id,
            source_filename=source_filename,
        )
    except Exception as exc:
        logger.error(
            "Deficiency queue duplicate check failed — vendor_id=%s certificate_id=%s error=%s",
            vendor_id,
            certificate_id,
            exc,
        )
        return

    if existing_id:
        logger.info(
            "Deficiency queue skipped duplicate active unsent request — vendor_id=%s certificate_id=%s existing_queue_id=%s",
            vendor_id,
            certificate_id,
            existing_id,
        )
        try:
            certs_table.update(certificate_id, {"Deficiency Email Queue Status": "Queued"})
        except Exception as exc:
            logger.info(
                "Certificate deficiency queue status write skipped (field likely missing) — certificate_id=%s error=%s",
                certificate_id,
                exc,
            )
        return

    template = _fetch_deficiency_template_record(templates_table)
    reason_lines = _build_deficiency_reason_lines(failure_reasons)
    compliance_failure_reasons_text = (
        "\n".join(reason_lines)
        if reason_lines
        else "- Please review all policy requirement deficiencies."
    )

    if template:
        logging.info("Deficiency template loaded from Airtable")
        subject_template = template.get("Subject") or _build_deficiency_email_subject(client_name)
        body_template = template.get("Body") or _build_deficiency_email_body(
            vendor_name=vendor_name,
            client_name=client_name,
            failure_reasons=failure_reasons,
            certificate_id=certificate_id,
            source_filename=source_filename,
        )
        subject = _apply_deficiency_template_placeholders(
            template_text=subject_template,
            vendor_name=vendor_name,
            client_name=client_name,
            compliance_failure_reasons_text=compliance_failure_reasons_text,
        )
        body = _apply_deficiency_template_placeholders(
            template_text=body_template,
            vendor_name=vendor_name,
            client_name=client_name,
            compliance_failure_reasons_text=compliance_failure_reasons_text,
        )
    else:
        logging.info("Deficiency template not found — using fallback")
        subject = _build_deficiency_email_subject(client_name)
        body = _build_deficiency_email_body(
            vendor_name=vendor_name,
            client_name=client_name,
            failure_reasons=failure_reasons,
            certificate_id=certificate_id,
            source_filename=source_filename,
        )

    queue_fields = {
        "Primary Email": vendor_email,
        "Subject": subject,
        "Body": body,
        "Email Type": "Deficiency Request",
        "Email Status": "Pending",
        "Record Status": "Active",
        "Vendor": [vendor_id],
    }
    if matched_client_id:
        queue_fields["Client"] = [matched_client_id]
        logger.info(
            "Deficiency queue payload includes client link — client_id=%s",
            matched_client_id,
        )

    try:
        queue_record = email_queue_table.create(queue_fields)
        logger.info(
            "Deficiency queue created — queue_id=%s vendor_id=%s certificate_id=%s",
            queue_record.get("id"),
            vendor_id,
            certificate_id,
        )
    except Exception as exc:
        logger.error(
            "Deficiency queue creation error — vendor_id=%s certificate_id=%s error=%s",
            vendor_id,
            certificate_id,
            exc,
        )
        return

    try:
        certs_table.update(certificate_id, {"Deficiency Email Queue Status": "Queued"})
    except Exception as exc:
        logger.info(
            "Certificate deficiency queue status write skipped (field likely missing) — certificate_id=%s error=%s",
            certificate_id,
            exc,
        )


def _extract_linked_record_ids(fields: dict, candidate_field_names: list[str]) -> list[str]:
    """Return linked record IDs from the first populated candidate field name."""
    for field_name in candidate_field_names:
        value = fields.get(field_name)
        if isinstance(value, list):
            linked_ids: list[str] = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    linked_ids.append(item)
                elif isinstance(item, dict):
                    item_id = item.get("id")
                    if isinstance(item_id, str) and item_id.strip():
                        linked_ids.append(item_id)
            return linked_ids
        if isinstance(value, str) and value.strip():
            return [value.strip()]
    return []


def _escape_formula_find_value(value: str) -> str:
    """Escape a value for Airtable FIND("...") formula usage."""
    return str(value or "").replace("\\", "\\\\").replace('"', '\\"')


def _log_client_requirements_airtable_debug(requirements_table, client_requirements_formula: str) -> None:
    """Log low-level Airtable request/response details for Client Requirements fetch debugging."""
    try:
        records_url = str(requirements_table.urls.records)
    except Exception:
        records_url = ""

    query_string = urlencode({"filterByFormula": client_requirements_formula})
    full_request_url = f"{records_url}?{query_string}" if records_url else "(unavailable)"

    logger.info(
        "Client Requirements debug — configured_table_name=%s expected_table_name=%s client_link_field_used=%s",
        TABLE_REQUIREMENTS,
        "Client Requirements",
        "Client Link",
    )
    logger.info(
        "Client Requirements debug — filterByFormula=%s",
        client_requirements_formula,
    )
    logger.info(
        "Client Requirements debug — full_request_url=%s",
        full_request_url,
    )

    api_key = getattr(getattr(requirements_table, "api", None), "api_key", None)
    if not records_url or not api_key:
        logger.warning(
            "Client Requirements debug — unable to fetch raw response (records_url or api_key unavailable)."
        )
        return

    try:
        raw_response = requests.get(
            records_url,
            headers={"Authorization": f"Bearer {api_key}"},
            params={"filterByFormula": client_requirements_formula},
            timeout=30,
        )
        logger.info(
            "Client Requirements debug — raw Airtable response JSON (status=%s): %s",
            raw_response.status_code,
            raw_response.text,
        )

        try:
            raw_json = raw_response.json()
            raw_records = raw_json.get("records", []) if isinstance(raw_json, dict) else []
            for idx, record in enumerate(raw_records, start=1):
                client_link_value = (record.get("fields", {}) or {}).get("Client Link")
                logger.info(
                    "Client Requirements debug — raw record %d Client Link field value=%s",
                    idx,
                    client_link_value,
                )
        except Exception as exc:
            logger.warning(
                "Client Requirements debug — failed to parse raw response JSON for Client Link logging: %s",
                exc,
            )
    except Exception as exc:
        logger.warning(
            "Client Requirements debug — raw Airtable request failed: %s",
            exc,
        )


def fetch_requirement_context_for_evaluator(
    requirements_table,
    request_records: list,
    client_records: list,
    matched_request_id: Optional[str],
    matched_client_id: Optional[str],
) -> dict:
    """Fetch safest available requirement context for evaluator payload enrichment.

    Priority:
      1) Requirements explicitly linked on matched request
      2) Requirements linked on matched client, then requirements linked by Client Link
    """
    try:
        requirement_records = requirements_table.all()
    except Exception as exc:
        logger.warning("Requirement context fetch failed (continuing without enrichment): %s", exc)
        return {}

    requirements_by_id = {
        record.get("id"): record
        for record in requirement_records
        if isinstance(record, dict) and isinstance(record.get("id"), str)
    }

    request_requirement_field_candidates = [
        "Requirements",
        "Client Requirements",
        "Requirement",
        "Requirement Link",
        "Requirements Link",
    ]
    client_requirement_field_candidates = [
        "Requirements",
        "Client Requirements",
        "Requirement",
        "Requirement Link",
        "Requirements Link",
    ]
    requirement_client_link_field_candidates = [
        "Client Link",
        "Client",
        "Clients",
    ]

    if matched_client_id:
        logger.info(
            "Compliance requirement enrichment — matched_client_id=%s",
            matched_client_id,
        )

        matched_client_record = next(
            (record for record in client_records if record.get("id") == matched_client_id),
            None,
        )
        matched_client_fields = (matched_client_record or {}).get("fields", {})
        matched_client_name = matched_client_fields.get("Client Name")
        matched_certificate_holder = matched_client_fields.get("Certificate Holder")

        logger.info(
            "Client Requirements lookup values — matched_client_id=%s matched_client_name=%s matched_certificate_holder=%s",
            matched_client_id,
            matched_client_name,
            matched_certificate_holder,
        )

        find_clauses = []
        for candidate_value in (
            matched_client_id,
            matched_client_name,
            matched_certificate_holder,
        ):
            if isinstance(candidate_value, str) and candidate_value.strip():
                escaped_candidate = _escape_formula_find_value(candidate_value.strip())
                find_clauses.append(
                    f'FIND("{escaped_candidate}", ARRAYJOIN({{Client Link}}))'
                )

        if len(find_clauses) > 1:
            client_requirements_formula = "OR(" + ",".join(find_clauses) + ")"
        elif find_clauses:
            client_requirements_formula = find_clauses[0]
        else:
            client_requirements_formula = ""

        logger.info(
            "Client Requirements lookup formula — final_filterByFormula=%s",
            client_requirements_formula,
        )

        if not client_requirements_formula:
            logger.warning(
                "Client Requirements lookup skipped — no non-empty values available for filterByFormula"
            )
            return {}

        _log_client_requirements_airtable_debug(
            requirements_table=requirements_table,
            client_requirements_formula=client_requirements_formula,
        )
        try:
            client_requirement_records = requirements_table.all(
                formula=client_requirements_formula
            )
            for idx, record in enumerate(client_requirement_records, start=1):
                client_link_value = (record.get("fields", {}) or {}).get("Client Link")
                logger.info(
                    "Client Requirements debug — parsed record %d Client Link field value=%s",
                    idx,
                    client_link_value,
                )
            client_requirements_from_table = []
            for record in client_requirement_records:
                fields = record.get("fields", {}) if isinstance(record, dict) else {}
                client_requirements_from_table.append(
                    {
                        "Policy Type": fields.get("Policy Type"),
                        "Required": fields.get("Required"),
                        "Minimum Limit": fields.get("Minimum Limit"),
                    }
                )

            required_policy_types = sorted(
                {
                    str(item.get("Policy Type")).strip()
                    for item in client_requirements_from_table
                    if item.get("Policy Type")
                }
            )

            logger.info(
                "Client Requirements fetch — matched_client_id=%s raw_records_count=%d requirement_count=%d required_policy_types=%s",
                matched_client_id,
                len(client_requirement_records),
                len(client_requirements_from_table),
                required_policy_types,
            )

            if client_requirements_from_table:
                logger.info(
                    "Compliance requirement enrichment — matched_client_id=%s requirement_count=%d required_policy_types=%s requirement_source=%s",
                    matched_client_id,
                    len(client_requirements_from_table),
                    required_policy_types,
                    "client_requirements_table",
                )
                return {
                    "matched_client": {
                        "requirements": client_requirements_from_table,
                    },
                    "client_requirements": client_requirements_from_table,
                }
        except Exception as exc:
            logger.warning(
                "Compliance requirement enrichment query failed for matched client — client_id=%s error=%s",
                matched_client_id,
                exc,
            )

    if matched_request_id:
        matched_request = next(
            (record for record in request_records if record.get("id") == matched_request_id),
            None,
        )
        if matched_request:
            request_fields = matched_request.get("fields", {})
            linked_requirement_ids = _extract_linked_record_ids(
                request_fields,
                request_requirement_field_candidates,
            )
            request_requirements = [
                requirements_by_id[req_id].get("fields", {})
                for req_id in linked_requirement_ids
                if req_id in requirements_by_id
            ]
            if request_requirements:
                logger.info(
                    "Compliance requirement enrichment — matched_client_id=%s requirement_count=%d requirement_source=%s",
                    matched_client_id,
                    len(request_requirements),
                    "matched_client_request.requirements",
                )
                return {
                    "matched_client_request": {
                        "requirements": request_requirements,
                    },
                    "request_requirements": request_requirements,
                }

    if matched_client_id:
        matched_client = next(
            (record for record in client_records if record.get("id") == matched_client_id),
            None,
        )
        client_requirements = []

        if matched_client:
            client_fields = matched_client.get("fields", {})
            linked_requirement_ids = _extract_linked_record_ids(
                client_fields,
                client_requirement_field_candidates,
            )
            client_requirements = [
                requirements_by_id[req_id].get("fields", {})
                for req_id in linked_requirement_ids
                if req_id in requirements_by_id
            ]

        if not client_requirements:
            # Keep parity with existing project pattern used in requirement validator:
            # requirements filtered by "Client Link" linked record.
            client_link_filtered_requirements = []
            for requirement_record in requirement_records:
                requirement_fields = requirement_record.get("fields", {})
                client_link_value = requirement_fields.get("Client Link")
                if (
                    isinstance(client_link_value, list)
                    and client_link_value
                    and client_link_value[0] == matched_client_id
                ):
                    client_link_filtered_requirements.append(requirement_fields)
            if client_link_filtered_requirements:
                client_requirements = client_link_filtered_requirements

        if not client_requirements:
            for requirement_record in requirement_records:
                requirement_fields = requirement_record.get("fields", {})
                linked_client_ids = _extract_linked_record_ids(
                    requirement_fields,
                    requirement_client_link_field_candidates,
                )
                if matched_client_id in linked_client_ids:
                    client_requirements.append(requirement_fields)

        if client_requirements:
            logger.info(
                "Compliance requirement enrichment — matched_client_id=%s requirement_count=%d requirement_source=%s",
                matched_client_id,
                len(client_requirements),
                "matched_client.requirements",
            )
            return {
                "matched_client": {
                    "requirements": client_requirements,
                },
                "client_requirements": client_requirements,
            }

    logger.info(
        "Compliance requirement enrichment — matched_client_id=%s requirement_count=%d requirement_source=%s",
        matched_client_id,
        0,
        "none",
    )
    return {}


def fetch_newest_imported(table) -> Optional[dict]:
    """Return the most-recent Incoming Extraction with status 'Imported', or None.

    Fetches all Imported records then sorts in Python, using:
      1. "Extraction Processed At" field if present
      2. Airtable's built-in createdTime as fallback
    This avoids pyairtable version-specific sort= format issues.
    """
    records = table.all(formula="{Processing Status} = 'Imported'")
    if not records:
        return None

    def sort_key(record):
        # Prefer the explicit timestamp field; fall back to Airtable createdTime
        ts = record["fields"].get("Extraction Processed At") or ""
        if not ts:
            ts = record.get("createdTime") or ""
        return ts

    return max(records, key=sort_key)


def find_vendor(vendors_table, named_insured: str) -> Optional[dict]:
    """Case-insensitive match of named_insured against Vendor Name field."""
    needle = named_insured.strip().lower()
    all_vendors = vendors_table.all()
    for vendor in all_vendors:
        vendor_name = vendor["fields"].get("Vendor Name", "").strip().lower()
        if vendor_name == needle:
            return vendor
    return None


def find_vendor_alias_matches(vendors_table, named_insured: str) -> list:
    """Case-insensitive exact match against comma-separated Vendors.Aliases values."""
    needle = named_insured.strip().lower()
    if not needle:
        return []

    matches = []
    all_vendors = vendors_table.all()
    for vendor in all_vendors:
        aliases_raw = vendor.get("fields", {}).get("Aliases", "")
        if not isinstance(aliases_raw, str) or not aliases_raw.strip():
            continue

        aliases = [part.strip().lower() for part in aliases_raw.split(",") if part.strip()]
        if needle in aliases:
            matches.append(vendor)

    return matches


def create_vendor(vendors_table, named_insured: str) -> dict:
    """Create a minimal vendor record from the extracted Named Insured value."""
    record = vendors_table.create(
        {
            "Vendor Name": named_insured.strip(),
            "Created By": "System",
            "Source": "COI Intake",
            "Status": "Unreviewed",
        }
    )
    logger.info(
        "Created Vendor — ID: %s  Name: '%s'",
        record["id"],
        record.get("fields", {}).get("Vendor Name", ""),
    )
    logger.info("Auto-created vendor marked as system-created/unreviewed.")
    return record


def policy_already_exists(policies_table, policy_number: str) -> bool:
    """Return True if a policy with this number already exists."""
    # Escape single quotes in the policy number for formula safety
    safe_num = policy_number.replace("'", "\\'")
    formula = f"{{Policy Number}} = '{safe_num}'"
    existing = policies_table.first(formula=formula)
    return existing is not None


def get_existing_policy_by_number(policies_table, policy_number: str) -> Optional[dict]:
    """Return existing policy record for this number, or None."""
    safe_num = policy_number.replace("'", "\\'")
    formula = f"{{Policy Number}} = '{safe_num}'"
    return policies_table.first(formula=formula)


def set_processing_status(incoming_table, record_id: str, status: str) -> None:
    """Update the Processing Status field on an Incoming Extractions record."""
    incoming_table.update(record_id, {"Processing Status": status})
    logger.info("Incoming Extraction %s → Processing Status = '%s'", record_id, status)


def set_vendor_expired_status(vendors_table, vendor_id: str) -> None:
    """Mark vendor as action needed for cancellation handling when policy is unknown."""
    vendors_table.update(vendor_id, {"Status": "Action Needed"})
    logger.info("Vendor %s → Status = 'Action Needed'", vendor_id)


def set_policy_canceled_status(policies_table, policy_id: str) -> None:
    """Mark a policy as canceled for cancellation handling."""
    policies_table.update(policy_id, {"Status": "Canceled"})
    logger.info("Policy %s → Status = 'Canceled'", policy_id)


def normalize_document_type(raw: str) -> str:
    """Normalize extracted document type for deterministic matching."""
    value = (raw or "").strip().lower()
    value = value.replace("_", " ").replace("-", " ")
    value = re.sub(r"\s+", " ", value)
    return value


def is_cancellation_document(document_type: str) -> bool:
    """Return True if the normalized document type is cancellation-related."""
    return document_type in {
        "cancellation",
        "cancellation notice",
        "notice of cancellation",
    }


def extract_policy_numbers(raw_data: dict) -> list:
    """Extract candidate policy numbers from top-level and nested policy JSON."""
    numbers = []
    top_level = (raw_data.get("policy_number") or "").strip()
    if top_level:
        numbers.append(top_level)

    for policy in raw_data.get("policies") or []:
        number = (policy.get("policy_number") or "").strip()
        if number:
            numbers.append(number)

    # Preserve order while removing duplicates
    return list(dict.fromkeys(numbers))


def find_existing_policy_by_number(policies_table, policy_numbers: list) -> Optional[dict]:
    """Return the first existing policy matching any candidate policy number."""
    for policy_number in policy_numbers:
        safe_num = policy_number.replace("'", "\\'")
        formula = f"{{Policy Number}} = '{safe_num}'"
        existing = policies_table.first(formula=formula)
        if existing:
            return existing
    return None


def parse_expiration_date(raw_value: str) -> Optional[date]:
    """Parse extracted expiration date using simple deterministic formats."""
    value = (raw_value or "").strip()
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y"):
        try:
            parsed = datetime.strptime(value, fmt).date()
            if fmt == "%m/%d/%y":
                logger.info(
                    "Parsed expiration date using 2-digit year format (%%m/%%d/%%y): raw=%s parsed=%s",
                    value,
                    parsed.isoformat(),
                )
            return parsed
        except ValueError:
            continue
    return None


def compute_policy_status(expiration_raw: str, policy_number: str = "") -> str:
    """Compute policy status from expiration date relative to today."""
    expiration = parse_expiration_date(expiration_raw)
    if not expiration:
        logger.warning(
            "Policy expiration date missing/unparseable — policy_number=%s raw_expiration=%s resulting_status=Needs Review",
            (policy_number or "").strip() or "(blank)",
            (expiration_raw or "").strip() or "(blank)",
        )
        return "Needs Review"

    today = date.today()
    if expiration < today:
        return "Expired"
    if (expiration - today).days <= 30:
        return "Expiring Soon"
    return "Current"


# ── Policy type normalisation ─────────────────────────────────────────────────

_POLICY_TYPE_MAP = {
    # General Liability
    "commercial general liability": "General Liability",
    "general liability":            "General Liability",
    "general liab":                 "General Liability",
    "cgl":                          "General Liability",
    "gl":                           "General Liability",
    "liability":                    "General Liability",
    "professional liability":       "General Liability",
    "products completed operations": "General Liability",
    "products/completed operations": "General Liability",
    # Workers Comp
    "workers compensation":         "Workers Comp",
    "workers comp":                 "Workers Comp",
    "workers' compensation":        "Workers Comp",
    "worker's compensation":        "Workers Comp",
    "work comp":                    "Workers Comp",
    "wc":                           "Workers Comp",
    "employers liability":          "Workers Comp",
    "employers' liability":         "Workers Comp",
    "employer's liability":         "Workers Comp",
    "employers liab":               "Workers Comp",
    "workers compensation and employers' liability": "Workers Comp",
    "workers compensation and employers liability": "Workers Comp",
    "workers' compensation and employers' liability": "Workers Comp",
    # Auto Liability
    "automobile liability":         "Auto Liability",
    "auto liability":               "Auto Liability",
    "auto liab":                    "Auto Liability",
    "commercial auto":              "Auto Liability",
    "commercial automobile":        "Auto Liability",
    "business auto":                "Auto Liability",
    "hired and non-owned auto":     "Auto Liability",
    "hired and non owned auto":     "Auto Liability",
    "hired auto":                   "Auto Liability",
    "non-owned auto":               "Auto Liability",
    "non owned auto":               "Auto Liability",
    "any auto":                     "Auto Liability",
    # Umbrella / Excess
    "umbrella liability":           "Umbrella",
    "umbrella liab":                "Umbrella",
    "umbrella":                     "Umbrella",
    "excess liability":             "Umbrella",
    "excess liab":                  "Umbrella",
    "excess":                       "Umbrella",
    "umbrella/excess":              "Umbrella",
    "excess/umbrella":              "Umbrella",
}


def normalize_policy_type(raw: str) -> str:
    """Map raw extracted policy type to an Airtable-approved select value.

    Falls back to 'General Liability' for unrecognised types so that every
    policy always gets a valid Airtable single-select value.
    """
    return _POLICY_TYPE_MAP.get(raw.strip().lower(), "General Liability")


def build_compliance_failure_reasons_json(failure_reasons: list) -> str:
    """Serialize structured failure reasons for Airtable long-text JSON storage."""
    serialized = [asdict(reason) for reason in (failure_reasons or [])]
    return json.dumps(serialized, ensure_ascii=False)


# ── Core steps ────────────────────────────────────────────────────────────────

def process_policies(
    policies_table,
    policies: list,
    vendor_record_id: str,
    certificate_record_id: str,
    source_filename: str,
) -> list:
    """
    Create an Insurance Policy record for each policy in the list.
    Skips duplicates by Policy Number.
    Returns list of policy record IDs touched in this run.
    """
    touched_ids = []
    logger.info("Insurance Policy vendor field in use: Vendor Link")
    for idx, policy in enumerate(policies, start=1):
        policy_number = (policy.get("policy_number") or "").strip()
        raw_type      = (policy.get("policy_type")   or "").strip()
        expiration_raw = (policy.get("expiration_date") or "").strip()
        computed_status = compute_policy_status(expiration_raw, policy_number)
        policy_type   = normalize_policy_type(raw_type)
        logger.info("Policy %d type: '%s' → '%s'", idx, raw_type, policy_type)
        logger.info(
            "Policy status computed — policy_number=%s expiration_date=%s status=%s",
            policy_number or "(blank)",
            expiration_raw or "(blank)",
            computed_status,
        )

        if not policy_number:
            logger.warning("Policy %d has no policy number.", idx)

        existing_policy = None
        if policy_number:
            existing_policy = get_existing_policy_by_number(policies_table, policy_number)
        if existing_policy:
            refreshed_fields = {
                "Status": computed_status,
            }
            if expiration_raw:
                refreshed_fields["Expiration Date"] = expiration_raw

            existing_vendor_links = existing_policy.get("fields", {}).get("Vendor Link")
            if not isinstance(existing_vendor_links, list) or not existing_vendor_links:
                refreshed_fields["Vendor Link"] = [vendor_record_id]
                logger.info(
                    "Vendor link repaired on existing policy update — Policy %d (%s) Vendor ID: %s",
                    idx,
                    policy_number,
                    vendor_record_id,
                )

            existing_certificate_links = existing_policy.get("fields", {}).get("Insurance Certificates")
            if not isinstance(existing_certificate_links, list) or not existing_certificate_links:
                refreshed_fields["Insurance Certificates"] = [certificate_record_id]
                logger.info(
                    "Certificate link repaired on existing policy update — Policy %d (%s) Certificate ID: %s",
                    idx,
                    policy_number,
                    certificate_record_id,
                )

            if (
                "Vendor Link" in refreshed_fields
                or "Insurance Certificates" in refreshed_fields
            ):
                logger.info(
                    "Existing policy relationships repaired — Policy %d (%s)",
                    idx,
                    policy_number,
                )

            if "coverage_limits" in policy:
                refreshed_fields["Coverage Limits"] = (policy.get("coverage_limits") or "").strip()
            if source_filename:
                refreshed_fields["Certificate Source Filename"] = source_filename

            policies_table.update(existing_policy["id"], refreshed_fields)
            logger.info(
                "Policy %d (%s) existing policy found — refreshed fields: %s",
                idx,
                policy_number,
                ", ".join(refreshed_fields.keys()),
            )
            touched_ids.append(existing_policy["id"])
            continue

        effective_raw = (policy.get("effective_date") or "").strip()

        fields = {
            "Policy Record":           f"{policy_type} — {policy_number}",
            "Vendor Link":             [vendor_record_id],
            "Insurance Certificates":  [certificate_record_id],
            "Policy Type":             policy_type,
            "Policy Number":           policy_number,
            "Carrier":                 (policy.get("carrier")          or "").strip(),
            "Coverage Limits":         (policy.get("coverage_limits")  or "").strip(),
            "Status":                  computed_status,
            "Certificate Source Filename": source_filename,
        }
        if effective_raw:
            fields["Effective Date"] = effective_raw
        if expiration_raw:
            fields["Expiration Date"] = expiration_raw

        logger.info(
            "Vendor link added on create — Policy %d (%s) Vendor ID: %s",
            idx,
            policy_number,
            vendor_record_id,
        )
        logger.info(
            "Certificate link added on create — Policy %d (%s) Certificate ID: %s",
            idx,
            policy_number,
            certificate_record_id,
        )

        record = policies_table.create(fields)
        logger.info(
            "Created Insurance Policy — ID: %s  Policy #: %s",
            record["id"], policy_number,
        )
        touched_ids.append(record["id"])

    return touched_ids


def create_certificate(
    certs_table,
    vendor_record_id: str,
    named_insured: str,
    source_filename: str,
    certificate_date: str,
    policy_record_ids: list,
    matched_request_id: Optional[str] = None,
    matched_client_id: Optional[str] = None,
) -> dict:
    """Create one Insurance Certificate record linked to the Vendor."""
    fields = {
        "Vendor Link":      [vendor_record_id],
        "Named Insured":    named_insured,
        "Source Filename":  source_filename,
    }
    if certificate_date and certificate_date.strip():
        fields["Certificate Date"] = certificate_date.strip()
    if matched_request_id:
        fields["COI Requests"] = [matched_request_id]
    if matched_client_id:
        fields["Client"] = [matched_client_id]
    # Link to policies if the table has a linked-record field for them
    if policy_record_ids:
        fields["Insurance Policies"] = policy_record_ids

    record = certs_table.create(fields)
    logger.info("Created Insurance Certificate — ID: %s", record["id"])
    return record


# ── Entry point ───────────────────────────────────────────────────────────────

def run():
    logger.info("=== Module 4: COI Processor starting ===")

    # ── Validate env ──────────────────────────────────────────────────────────
    api_key = (config.AIRTABLE_API_KEY or "").strip()
    raw_base = (config.AIRTABLE_BASE_ID or "").strip()

    if not api_key or not raw_base:
        raise EnvironmentError(
            "AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env"
        )

    base_id = clean_base_id(raw_base)
    logger.info("Airtable base ID : %s", base_id)

    api    = Api(api_key)
    tables = get_tables(api, base_id)

    # ── Step 1: fetch newest Imported extraction ───────────────────────────────
    logger.info("Step 1 — Fetching newest 'Imported' record from '%s'...", TABLE_INCOMING)
    extraction = fetch_newest_imported(tables[TABLE_INCOMING])

    if not extraction:
        logger.info("No records with Processing Status = 'Imported' found. Nothing to do.")
        return

    extraction_id     = extraction["id"]
    extraction_fields = extraction["fields"]
    vendor = None

    named_insured     = (extraction_fields.get("Named Insured") or "").strip()
    source_filename   = (extraction_fields.get("Source Filename") or "").strip()
    raw_json_str      = extraction_fields.get("Raw JSON") or ""

    logger.info("Found extraction ID : %s", extraction_id)
    logger.info("Named Insured       : %s", named_insured)
    logger.info("Source Filename     : %s", source_filename)

    # ── Matcher integration: evaluate and write Incoming Extractions match fields ──
    vendor_records = tables[TABLE_VENDORS].all()
    policy_records = tables[TABLE_POLICIES].all()
    client_records = tables[TABLE_CLIENTS].all()
    request_records = tables[TABLE_REQUESTS].all()

    match_evaluation = evaluate_vendor_match(
        extraction_fields=extraction_fields,
        vendor_records=vendor_records,
        policy_records=policy_records,
        client_records=client_records,
        request_records=request_records,
    )
    match_update_written = apply_incoming_extraction_match_update(
        incoming_extractions_table=tables[TABLE_INCOMING],
        record_id=extraction_id,
        existing_fields=extraction_fields,
        evaluation=match_evaluation,
    )
    logger.info(
        "Matcher result — record_id=%s match_status=%s applied_rule_id=%s update_written=%s",
        extraction_id,
        match_evaluation.match_status,
        match_evaluation.applied_rule_id,
        match_update_written,
    )

    def first_linked_id(value) -> Optional[str]:
        if isinstance(value, list) and value:
            first = value[0]
            return first if isinstance(first, str) and first.strip() else None
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    matched_vendor_id = (
        first_linked_id(extraction_fields.get("Matched Vendor"))
        or match_evaluation.matched_vendor_id
    )
    matched_client_id = (
        first_linked_id(extraction_fields.get("Matched Client"))
        or match_evaluation.matched_client_id
    )
    matched_request_id = (
        first_linked_id(extraction_fields.get("Matched Client Request"))
        or match_evaluation.matched_request_id
    )

    if matched_vendor_id:
        vendor = next(
            (v for v in vendor_records if v.get("id") == matched_vendor_id),
            {"id": matched_vendor_id, "fields": {}},
        )
        logger.info("Using matcher-resolved vendor: %s", matched_vendor_id)
    if matched_client_id:
        logger.info("Using matcher-resolved client: %s", matched_client_id)
    if matched_request_id:
        logger.info("Using matcher-resolved client request: %s", matched_request_id)

    if matched_request_id and matched_client_id:
        matched_request = next(
            (record for record in request_records if record.get("id") == matched_request_id),
            None,
        )
        request_client_value = (matched_request or {}).get("fields", {}).get("Client")
        has_request_client = (
            isinstance(request_client_value, list) and any(request_client_value)
        ) or (isinstance(request_client_value, str) and request_client_value.strip())
        if not has_request_client:
            try:
                tables[TABLE_REQUESTS].update(matched_request_id, {"Client": [matched_client_id]})
                logger.info(
                    "Backfilled COI Request Client link — request_id=%s client_id=%s",
                    matched_request_id,
                    matched_client_id,
                )
            except Exception as exc:
                logger.warning(
                    "COI Request Client backfill skipped — request_id=%s error=%s",
                    matched_request_id,
                    exc,
                )

    # ── Step 2: parse Raw JSON ────────────────────────────────────────────────
    logger.info("Step 2 — Parsing Raw JSON...")
    try:
        raw_data = json.loads(raw_json_str) if raw_json_str else {}
    except json.JSONDecodeError as exc:
        logger.error("Raw JSON is not valid JSON: %s", exc)
        set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
        return

    policies         = raw_data.get("policies") or []
    certificate_date = raw_data.get("certificate_date") or ""
    document_type    = normalize_document_type(raw_data.get("document_type") or "")
    logger.info("Policies in JSON : %d", len(policies))
    logger.info("Document Type    : %s", document_type or "(blank)")

    if match_evaluation.match_status == "Pending Match":
        logger.info(
            "Match status is Pending Match — proceeding with auto-create vendor flow."
        )
        if not vendor:
            if not named_insured:
                logger.warning(
                    "Pending Match but Named Insured is blank. Setting status to 'Needs Review'."
                )
                set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
                return

            vendor = create_vendor(tables[TABLE_VENDORS], named_insured)
            logger.info(
                "Pending Match vendor handling complete — vendor auto-created and processing will continue."
            )
    elif match_evaluation.match_status != "Matched":
        logger.info(
            "Skipping vendor resolution — match status is %s",
            match_evaluation.match_status,
        )
        set_processing_status(tables[TABLE_INCOMING], extraction_id, "Processed")
        return

    # ── Step 3: match Vendor ──────────────────────────────────────────────────
    if not vendor:
        logger.info("Step 3 — Looking up Vendor for Named Insured: '%s'...", named_insured)
        vendor = find_vendor(tables[TABLE_VENDORS], named_insured)

        if not vendor:
            alias_matches = find_vendor_alias_matches(tables[TABLE_VENDORS], named_insured)
            if len(alias_matches) == 1:
                vendor = alias_matches[0]
                logger.info(
                    "Vendor matched by exact alias — ID: %s  Name: '%s'",
                    vendor["id"],
                    vendor.get("fields", {}).get("Vendor Name", ""),
                )
            elif len(alias_matches) > 1:
                logger.warning(
                    "Ambiguous vendor alias match for '%s' (matches=%d). Setting status to 'Needs Review'.",
                    named_insured,
                    len(alias_matches),
                )
                set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
                logger.info("=== Module 4 complete (ambiguous alias match) ===")
                return

        if not vendor:
            if not named_insured:
                logger.warning(
                    "No Vendor match and Named Insured is blank. Setting status to 'Needs Review'."
                )
                set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
                logger.info("=== Module 4 complete (blank Named Insured) ===")
                return

            logger.warning(
                "No Vendor found matching '%s'. Auto-creating Vendor for V1 zero-touch flow.",
                named_insured,
            )
            vendor = create_vendor(tables[TABLE_VENDORS], named_insured)

    vendor_id   = vendor["id"]
    vendor_name = vendor["fields"].get("Vendor Name", "")
    logger.info("Vendor matched — ID: %s  Name: '%s'", vendor_id, vendor_name)

    # Minimal cancellation handling: update affected policy status if found,
    # otherwise flag the vendor for action needed. Continue normal processing.
    if is_cancellation_document(document_type):
        logger.info("Cancellation document detected (document_type='%s').", document_type)
        try:
            candidate_policy_numbers = extract_policy_numbers(raw_data)
            if not candidate_policy_numbers:
                logger.info(
                    "Cancellation handling: no policy numbers extracted; proceeding with vendor-level fallback (legacy trailing check removed as unused)."
                )
            affected_policy = find_existing_policy_by_number(
                tables[TABLE_POLICIES], candidate_policy_numbers
            )

            if affected_policy:
                logger.info(
                    "Cancellation affected policy found: %s",
                    affected_policy.get("fields", {}).get("Policy Number", "(unknown)"),
                )
                set_policy_canceled_status(tables[TABLE_POLICIES], affected_policy["id"])
                logger.info("Cancellation status updated: Policy Status set to 'Canceled'.")
            else:
                logger.info("Cancellation affected policy found: no")
                set_vendor_expired_status(tables[TABLE_VENDORS], vendor_id)
                logger.info("Cancellation status updated: Vendor Status set to 'Action Needed'.")
        except Exception as exc:
            logger.warning("Cancellation handling failed (continuing processing): %s", exc)

    # ── Step 4: create Insurance Certificate record ───────────────────────────
    logger.info("Step 4 — Creating Insurance Certificate record...")
    certificate_record = create_certificate(
        tables[TABLE_CERTS],
        vendor_id,
        named_insured,
        source_filename,
        certificate_date,
        [],
        matched_request_id,
        matched_client_id,
    )
    certificate_id = certificate_record["id"]
    if not matched_request_id:
        logger.info("No client request matched — certificate created without request linkage")

    # ── Step 5: create/update Insurance Policy records ────────────────────────
    logger.info("Step 5 — Creating Insurance Policy records...")
    if not policies:
        logger.warning("No policies found in Raw JSON — no policy records will be created.")

    touched_policy_ids = process_policies(
        tables[TABLE_POLICIES],
        policies,
        vendor_id,
        certificate_id,
        source_filename,
    )
    if touched_policy_ids:
        tables[TABLE_CERTS].update(certificate_id, {"Insurance Policies": touched_policy_ids})
        logger.info(
            "Certificate %s linked to policies from this run: %d",
            certificate_id,
            len(touched_policy_ids),
        )

    logger.info(
        "Policies created: %d  (skipped as duplicates: %d)",
        len([pid for pid in touched_policy_ids if pid]),
        max(len(policies) - len(touched_policy_ids), 0),
    )

    # ── Step 6: evaluator stub integration + cert compliance writeback ───────
    logger.info("Step 6 — Running returned COI compliance evaluator stub...")
    try:
        evaluator_raw_data = dict(raw_data)
        requirement_context = fetch_requirement_context_for_evaluator(
            requirements_table=tables[TABLE_REQUIREMENTS],
            request_records=request_records,
            client_records=client_records,
            matched_request_id=matched_request_id,
            matched_client_id=matched_client_id,
        )
        if requirement_context:
            for context_key, context_value in requirement_context.items():
                existing = evaluator_raw_data.get(context_key)
                if isinstance(existing, dict) and isinstance(context_value, dict):
                    merged = dict(existing)
                    merged.update(context_value)
                    evaluator_raw_data[context_key] = merged
                else:
                    evaluator_raw_data[context_key] = context_value
            logger.info(
                "Compliance payload raw_data enriched with requirement context keys: %s",
                ", ".join(sorted(requirement_context.keys())),
            )
        else:
            logger.info("No requirement context found for compliance payload enrichment.")

        compliance_payload = ReturnedCoiComplianceInput(
            extraction_id=extraction_id,
            vendor_id=vendor_id,
            client_id=matched_client_id,
            request_id=matched_request_id,
            certificate_id=certificate_id,
            policy_ids=touched_policy_ids,
            document_type=document_type,
            source_filename=source_filename,
            named_insured=named_insured,
            raw_data=evaluator_raw_data,
        )
        compliance_result = evaluate_returned_coi_compliance(compliance_payload)
        evaluator_required_policy_types = compliance_result.metadata.get("required_policy_types", [])
        evaluator_requirement_source = compliance_result.metadata.get("requirement_source", "none")
        logger.info(
            "Compliance evaluator requirement context — matched_client_id=%s required_policy_types=%s requirement_source=%s",
            matched_client_id,
            evaluator_required_policy_types,
            evaluator_requirement_source,
        )

        compliance_evaluated_at_utc_dt = datetime.utcnow().replace(microsecond=0)
        compliance_evaluated_at_utc = compliance_evaluated_at_utc_dt.isoformat() + "Z"
        compliance_evaluated_at_et = (
            compliance_evaluated_at_utc_dt
            .replace(tzinfo=pytz.utc)
            .astimezone(eastern)
            .strftime("%Y-%m-%d %I:%M:%S %p ET")
        )
        logger.info("Compliance Evaluated At (ET): %s", compliance_evaluated_at_et)

        compliance_update_fields = {
            COMPLIANCE_CERT_FIELD_NAMES["status"]: compliance_result.outcome,
            COMPLIANCE_CERT_FIELD_NAMES["evaluated_at"]: compliance_evaluated_at_utc,
            COMPLIANCE_CERT_FIELD_NAMES["decision_summary"]: compliance_result.decision_summary,
            COMPLIANCE_CERT_FIELD_NAMES["failure_reasons_json"]: build_compliance_failure_reasons_json(
                compliance_result.failure_reasons
            ),
        }
        tables[TABLE_CERTS].update(certificate_id, compliance_update_fields)
        logger.info(
            "Certificate %s compliance stub result written to Insurance Certificates.",
            certificate_id,
        )

        if compliance_result.outcome == "Non-Compliant":
            queue_deficiency_email_if_needed(
                email_queue_table=tables[TABLE_EMAIL_QUEUE],
                templates_table=tables[TABLE_TEMPLATES],
                certs_table=tables[TABLE_CERTS],
                vendor_id=vendor_id,
                vendor_fields=vendor.get("fields", {}),
                vendor_name=vendor_name,
                matched_client_id=matched_client_id,
                client_records=client_records,
                certificate_id=certificate_id,
                source_filename=source_filename,
                failure_reasons=compliance_result.failure_reasons,
            )
        else:
            logger.info(
                "Deficiency queue not triggered — compliance outcome=%s certificate_id=%s",
                compliance_result.outcome,
                certificate_id,
            )
    except Exception as exc:
        logger.error(
            "Compliance evaluator/writeback failed (continuing processing) for certificate %s: %s",
            certificate_id,
            exc,
        )
        logger.warning(
            "If this is an unknown-field Airtable error, add these Insurance Certificates fields to enable writeback: %s",
            ", ".join(COMPLIANCE_CERT_FIELD_NAMES.values()),
        )

    # ── Step 7: mark extraction as Processed ──────────────────────────────────
    logger.info("Step 7 — Marking extraction as 'Processed'...")
    set_processing_status(tables[TABLE_INCOMING], extraction_id, "Processed")

    logger.info("=== Module 4 complete ===")


if __name__ == "__main__":
    try:
        run()
    except (EnvironmentError, ValueError, RuntimeError) as exc:
        logger.error("Processor failed: %s", exc)
        raise SystemExit(1) from exc