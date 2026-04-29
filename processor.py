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
from dataclasses import asdict, dataclass, field as dc_field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal, Optional
from dotenv import load_dotenv
from pyairtable import Api
import pytz
import requests

from incoming_extraction_matcher_slice import (
    apply_incoming_extraction_match_update,
    evaluate_vendor_match,
)
# Minimal compliance result dataclasses (inlined from former returned_coi_compliance_evaluator.py)
@dataclass
class ComplianceFailureReason:
    code: str
    message: str
    severity: Literal["info", "warning", "error"] = "warning"
    field: Optional[str] = None
    policy_id: Optional[str] = None
    metadata: dict = dc_field(default_factory=dict)

@dataclass
class ReturnedCoiComplianceResult:
    outcome: str
    decision_summary: str
    failure_reasons: list = dc_field(default_factory=list)
    evaluator_version: str = "v2-module-7b"
    should_queue_deficiency_email: bool = False
    deficiency_email_payload: Optional[dict] = None
    metadata: dict = dc_field(default_factory=dict)
from module_7b_requirement_validator import (
    fetch_requirements_for_client,
    fetch_policies_for_vendor,
    fetch_active_assignments_for_vendor,
    evaluate_assignment,
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
TABLE_ASSIGNMENTS = "Vendor Client Assignments"


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
            TABLE_ASSIGNMENTS,
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
            lines.append(f"- {policy_type} certificate documentation is not on file")
        elif code == "REQUIRED_POLICY_EXPIRED":
            lines.append(f"- {policy_type} policy has expired and must be current")
        elif code == "REQUIRED_POLICY_LIMIT_BELOW_MINIMUM":
            lines.append(f"- {policy_type} limits shown on submitted certificate do not meet the required minimum")
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
        f"Your submitted Certificate of Insurance for {client_name} has been reviewed and documentation deficiencies have been identified.\n\n"
        "To proceed, please provide an updated COI addressing the following:\n\n"
        f"{reasons_block}\n\n"
        f"Please also ensure that {client_name} is listed as the certificate holder on the updated certificate.\n\n"
        "You may reply directly to this email with the updated COI.\n\n"
        "If you have any questions or need assistance, we’re happy to help.\n\n"
        "Best regards,  \n"
        "Carolina Compliance Solutions"
        "\n\n---\n"
        "This assessment is based solely on certificate of insurance documentation "
        "submitted to Carolina Compliance Solutions. It does not constitute verification "
        "of actual insurance coverage, policy terms, or carrier obligations. "
        "Please consult your insurance advisor for coverage determinations."
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
    vendor_name: str,
    certificate_id: str,
    source_filename: str,
) -> Optional[str]:
    # ARRAYJOIN on linked record fields returns display names, not record IDs.
    # Match on vendor name (the display value) instead.
    escaped_vendor_name = _escape_formula_value(vendor_name)
    formula = (
        "AND("
        "{Email Type}='Deficiency Request',"
        "{Record Status}='Active',"
        "NOT({Email Status}='Sent'),"
        f"FIND('{escaped_vendor_name}', ARRAYJOIN({{Vendor}}))"
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
            vendor_name=vendor_name,
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
            certs_table.update(certificate_id, {"Deficiency Email Queue Status": "Queued"}, typecast=True)
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

    from datetime import datetime, timezone
    send_after = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    queue_fields = {
        "Primary Email": vendor_email,
        "Subject": subject,
        "Body": body,
        "Email Type": "Deficiency Request",
        "Email Status": "Pending",
        "Record Status": "Active",
        "Reminder Status": "Queued",
        "Send After": send_after,
        "Vendor": [vendor_id],
    }
    if matched_client_id:
        queue_fields["Client"] = [matched_client_id]
        logger.info(
            "Deficiency queue payload includes client link — client_id=%s",
            matched_client_id,
        )

    try:
        queue_record = email_queue_table.create(queue_fields, typecast=True)
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
        certs_table.update(certificate_id, {"Deficiency Email Queue Status": "Queued"}, typecast=True)
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


def fetch_all_pending_extractions(table) -> list:
    """Return all Incoming Extractions with status 'Imported', sorted oldest-first.

    Processes oldest records first (FIFO) so no extraction gets stuck behind newer ones.
    """
    records = table.all(formula="{Processing Status} = 'Imported'")
    if not records:
        return []

    def sort_key(record):
        ts = record["fields"].get("Extraction Processed At") or ""
        if not ts:
            ts = record.get("createdTime") or ""
        return ts

    records.sort(key=sort_key)
    return records


def find_vendor(vendors_table, named_insured: str) -> Optional[dict]:
    """Case-insensitive exact match of named_insured against Vendor Name field.

    If multiple records share the same name, returns the oldest (by createdTime)
    and logs a warning about the duplicates.
    """
    needle = named_insured.strip().lower()
    if not needle:
        return None
    all_vendors = vendors_table.all()
    matches = []
    for vendor in all_vendors:
        vendor_name = vendor["fields"].get("Vendor Name", "").strip().lower()
        if vendor_name == needle:
            matches.append(vendor)
    if not matches:
        return None
    if len(matches) > 1:
        matches.sort(key=lambda v: v.get("createdTime", ""))
        logger.warning(
            "Duplicate vendor records found for '%s' (count=%d). Using oldest record %s.",
            named_insured,
            len(matches),
            matches[0]["id"],
        )
    return matches[0]


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


def named_insured_matches_client(clients_table, named_insured: str) -> bool:
    """Return True if the named insured matches an existing Client Name."""
    needle = named_insured.strip().lower()
    if not needle:
        return False
    all_clients = clients_table.all()
    for client in all_clients:
        client_name = client["fields"].get("Client Name", "").strip().lower()
        if client_name == needle:
            logger.warning(
                "Named insured '%s' matches Client '%s' (ID: %s) — not a vendor.",
                named_insured,
                client["fields"].get("Client Name", ""),
                client["id"],
            )
            return True
    return False


def create_vendor(vendors_table, named_insured: str) -> dict:
    """Create a minimal vendor record from the extracted Named Insured value."""
    record = vendors_table.create(
        {
            "Vendor Name": named_insured.strip(),
            "Created By": "System",
            "Source": "COI Intake",
            "Status": "Unreviewed",
        },
        typecast=True,
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


def get_existing_policies_by_vendor_and_type(
    policies_table, vendor_record_id: str, policy_type: str
) -> list:
    """Return all policy records for a vendor + policy type combination.

    Used for renewal detection: when a new COI arrives, we check if an older
    policy of the same type already exists so we can mark it superseded.
    Matches both normalized type (e.g. 'General Liability') and common raw
    variants (e.g. 'Commercial General Liability').
    """
    safe_vendor = vendor_record_id.replace("'", "\\'")
    safe_type = policy_type.replace("'", "\\'")

    # Build OR clause for type variants
    type_variants = {safe_type}
    _type_aliases = {
        "General Liability": ["Commercial General Liability", "CGL"],
        "Auto Liability": ["Automobile Liability", "Commercial Auto", "Commercial Automobile Liability"],
        "Workers Comp": ["Workers Compensation", "Workers Compensation and Employers' Liability",
                         "Workers Compensation And Employers' Liability"],
        "Umbrella": ["Umbrella Liab", "Umbrella Liability", "Excess Liability", "Umbrella Liab/Excess Liab"],
    }
    for alias in _type_aliases.get(policy_type, []):
        type_variants.add(alias.replace("'", "\\'"))

    if len(type_variants) == 1:
        type_clause = f"{{Policy Type}} = '{safe_type}'"
    else:
        type_parts = ", ".join(f"{{Policy Type}} = '{t}'" for t in type_variants)
        type_clause = f"OR({type_parts})"

    formula = (
        f"AND("
        f"FIND('{safe_vendor}', ARRAYJOIN({{Vendor Link}})), "
        f"{type_clause}, "
        f"{{Status}} != 'Superseded'"
        f")"
    )
    try:
        return policies_table.all(formula=formula)
    except Exception as exc:
        logger.warning("Failed to query existing policies for vendor %s type %s: %s",
                        vendor_record_id, policy_type, exc)
        return []


def _parse_date_safe(raw: str) -> Optional[date]:
    """Try to parse a date string; return None on failure."""
    cleaned = raw.strip() if raw else ""
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


# ── COI classification constants ──────────────────────────────────────────────
COI_DUPLICATE = "DUPLICATE"
COI_RENEWAL = "RENEWAL"
COI_MID_TERM_UPDATE = "MID_TERM_UPDATE"
COI_NEW_VENDOR = "NEW_VENDOR"


def classify_incoming_coi(
    new_policy: dict,
    existing_policies: list,
) -> tuple:
    """Classify a single incoming policy line against existing policy records.

    Args:
        new_policy: dict with keys: policy_number, policy_type, effective_date,
                    expiration_date, carrier, coverage_limits,
                    additional_insured_checked, waiver_of_subrogation_checked
        existing_policies: list of Airtable policy record dicts (with "fields" key)
                          for this vendor, NOT superseded

    Returns:
        (classification, matched_existing_record_or_None) where classification is
        one of COI_DUPLICATE, COI_RENEWAL, COI_MID_TERM_UPDATE, COI_NEW_VENDOR.
    """
    if not existing_policies:
        return COI_NEW_VENDOR, None

    new_number = (new_policy.get("policy_number") or "").strip().lower()
    new_type = (new_policy.get("policy_type") or "").strip().lower()
    new_carrier = (new_policy.get("carrier") or "").strip().lower()
    new_effective = _parse_date_safe(new_policy.get("effective_date") or "")
    new_limits = (new_policy.get("coverage_limits") or "").strip().lower()

    # ── Step 1: Match by policy number ───────────────────────────────────
    if new_number:
        for existing in existing_policies:
            ef = existing.get("fields", {})
            ex_number = (ef.get("Policy Number") or "").strip().lower()
            if not ex_number or ex_number != new_number:
                continue

            # Same policy number found — is it a duplicate or mid-term update?
            ex_effective = _parse_date_safe(ef.get("Effective Date") or "")
            ex_carrier = (ef.get("Carrier") or "").strip().lower()
            ex_limits = (ef.get("Coverage Limits") or "").strip().lower()
            ex_ai = bool(ef.get("Additional Insured"))
            ex_wos = bool(ef.get("Waiver"))

            new_ai = bool(new_policy.get("additional_insured_checked"))
            new_wos = bool(new_policy.get("waiver_of_subrogation_checked"))

            # True duplicate: same number + same effective + same carrier
            same_effective = (
                new_effective and ex_effective and new_effective == ex_effective
            ) or (not new_effective and not ex_effective)
            same_carrier = new_carrier == ex_carrier or not new_carrier or not ex_carrier

            if same_effective and same_carrier:
                # Check if limits/endorsements also match (true dup) or differ (mid-term)
                limits_changed = new_limits and ex_limits and new_limits != ex_limits
                endorsements_changed = (new_ai != ex_ai) or (new_wos != ex_wos)

                if limits_changed or endorsements_changed:
                    return COI_MID_TERM_UPDATE, existing
                return COI_DUPLICATE, existing

            # Same number but different effective or carrier → mid-term update
            return COI_MID_TERM_UPDATE, existing

    # ── Step 2: Match by type + carrier for renewal detection ────────────
    normalized_new_type = normalize_policy_type(new_type) if new_type else ""
    for existing in existing_policies:
        ef = existing.get("fields", {})
        ex_type_raw = (ef.get("Policy Type") or "").strip()
        ex_type_normalized = normalize_policy_type(ex_type_raw.lower()) if ex_type_raw else ""
        ex_carrier = (ef.get("Carrier") or "").strip().lower()
        ex_expiration = _parse_date_safe(ef.get("Expiration Date") or "")

        if ex_type_normalized != normalized_new_type and ex_type_raw != normalized_new_type:
            continue

        # Same type — check if this looks like a renewal
        same_carrier = new_carrier == ex_carrier or not new_carrier or not ex_carrier

        if same_carrier and new_effective and ex_expiration:
            # New effective date is at or after old expiration → renewal
            if new_effective >= ex_expiration:
                return COI_RENEWAL, existing

        # Same type, different carrier, new effective after old → also renewal (carrier switch)
        if new_effective and ex_expiration and new_effective >= ex_expiration:
            return COI_RENEWAL, existing

    # ── Step 3: No match found ───────────────────────────────────────────
    return COI_NEW_VENDOR, None


# ── Carrier normalization for continuity checks ──────────────────────────────

_CARRIER_STRIP_SUFFIXES = [
    "insurance company", "insurance co.", "insurance co",
    "ins. company", "ins. co.", "ins. co", "ins co",
    "insurance", "ins",
    "company", "co.", "co",
    "incorporated", "inc.", "inc",
    "corporation", "corp.", "corp",
    "limited", "ltd.", "ltd",
    "group", "llc", "l.l.c.",
]


def _normalize_carrier(name: str) -> str:
    """Normalize a carrier name for comparison.

    Iteratively strips common suffixes, punctuation, and whitespace so that
    'Travelers Insurance Company' and 'Travelers Ins. Co.' compare equal.
    """
    text = (name or "").strip().lower()
    text = text.replace(".", " ").replace(",", " ").replace("'", "")
    text = " ".join(text.split())
    # Strip suffixes iteratively — longest first, repeat until stable
    changed = True
    while changed:
        changed = False
        for suffix in sorted(_CARRIER_STRIP_SUFFIXES, key=len, reverse=True):
            if text.endswith(suffix):
                text = text[: -len(suffix)].strip()
                changed = True
                break
    return " ".join(text.split())


def check_carrier_continuity(
    new_policy_data: dict,
    existing_policy_record: dict,
) -> dict:
    """Check carrier and policy number continuity on a renewal.

    Pure function — returns a dict of findings, does not write to Airtable.

    Returns:
        {
            "carrier_changed": bool,
            "old_carrier": str,
            "new_carrier": str,
            "policy_number_changed": bool,
            "old_number": str,
            "new_number": str,
            "policy_type": str,
        }
    """
    ef = existing_policy_record.get("fields", {})

    old_carrier_raw = (ef.get("Carrier") or "").strip()
    new_carrier_raw = (new_policy_data.get("carrier") or "").strip()
    old_number = (ef.get("Policy Number") or "").strip()
    new_number = (new_policy_data.get("policy_number") or "").strip()
    policy_type = ef.get("Policy Type") or new_policy_data.get("policy_type") or ""

    carrier_changed = False
    if old_carrier_raw and new_carrier_raw:
        carrier_changed = _normalize_carrier(old_carrier_raw) != _normalize_carrier(new_carrier_raw)

    policy_number_changed = False
    if old_number and new_number:
        policy_number_changed = old_number.strip().lower() != new_number.strip().lower()

    return {
        "carrier_changed": carrier_changed,
        "old_carrier": old_carrier_raw,
        "new_carrier": new_carrier_raw,
        "policy_number_changed": policy_number_changed,
        "old_number": old_number,
        "new_number": new_number,
        "policy_type": policy_type,
    }


def set_processing_status(incoming_table, record_id: str, status: str) -> None:
    """Update the Processing Status field on an Incoming Extractions record."""
    incoming_table.update(record_id, {"Processing Status": status}, typecast=True)
    logger.info("Incoming Extraction %s → Processing Status = '%s'", record_id, status)


def set_vendor_expired_status(vendors_table, vendor_id: str) -> None:
    """Mark vendor as action needed for cancellation handling when policy is unknown."""
    vendors_table.update(vendor_id, {"Status": "Action Needed"}, typecast=True)
    logger.info("Vendor %s → Status = 'Action Needed'", vendor_id)


def set_policy_canceled_status(policies_table, policy_id: str) -> None:
    """Mark a policy as canceled for cancellation handling."""
    policies_table.update(policy_id, {"Status": "Canceled"}, typecast=True)
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


TBD_VALUES = {"tbd", "t.b.d", "to be determined", "pending",
               "tba", "to be assigned", "not yet assigned", "n/a"}


def is_invalid_policy_number(policy_number: str) -> bool:
    """Return True if the policy number is missing, TBD, or otherwise invalid."""
    if not policy_number:
        return True
    return str(policy_number).strip().lower() in TBD_VALUES


def extract_policy_numbers(raw_data: dict) -> list:
    """Extract candidate policy numbers from top-level and nested policy JSON.

    TBD / invalid policy numbers are excluded so they never feed into
    policy-number matching logic.
    """
    numbers = []
    top_level = (raw_data.get("policy_number") or "").strip()
    if top_level and not is_invalid_policy_number(top_level):
        numbers.append(top_level)

    for policy in raw_data.get("policies") or []:
        number = (policy.get("policy_number") or "").strip()
        if number and not is_invalid_policy_number(number):
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
    "auto":                         "Auto Liability",
    "cal":                          "Auto Liability",
    "commercial auto":              "Auto Liability",
    "commercial automobile":        "Auto Liability",
    "commercial auto liability":    "Auto Liability",
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
    incoming_table=None,
    extraction_id: str = "",
    certs_table=None,
) -> list:
    """
    Create an Insurance Policy record for each policy in the list.
    Skips duplicates by Policy Number.
    Returns list of policy record IDs touched in this run.
    """
    touched_ids = []
    new_records_created = 0
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

        if is_invalid_policy_number(policy_number):
            logger.warning(
                "Invalid policy number '%s' detected — marking extraction as Needs Review. "
                "TBD/pending policy numbers are not accepted.",
                policy_number or "(blank)",
            )
            if incoming_table and extraction_id:
                set_processing_status(incoming_table, extraction_id, "Needs Review")
            # Add failure reason to the Insurance Certificate record if one exists
            if certs_table and certificate_record_id:
                try:
                    cert_record = certs_table.get(certificate_record_id)
                    existing_reasons = cert_record.get("fields", {}).get("Compliance Failure Reasons", "") or ""
                    tbd_reason = "TBD or invalid policy number — policy not yet bound"
                    if tbd_reason not in existing_reasons:
                        updated_reasons = (
                            f"{existing_reasons}\n{tbd_reason}".strip()
                            if existing_reasons
                            else tbd_reason
                        )
                        certs_table.update(certificate_record_id, {
                            "Compliance Failure Reasons": updated_reasons,
                        })
                        logger.info(
                            "Added TBD failure reason to certificate %s",
                            certificate_record_id,
                        )
                except Exception as exc:
                    logger.warning(
                        "Failed to update compliance failure reasons on certificate %s: %s",
                        certificate_record_id, exc,
                    )
            continue

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

            policies_table.update(existing_policy["id"], refreshed_fields, typecast=True)
            logger.info(
                "Policy %d (%s) existing policy found — refreshed fields: %s",
                idx,
                policy_number,
                ", ".join(refreshed_fields.keys()),
            )
            touched_ids.append(existing_policy["id"])
            continue

        # ── Renewal detection: check for existing policies of same type ──────
        effective_raw = (policy.get("effective_date") or "").strip()
        new_effective = _parse_date_safe(effective_raw)

        if policy_type and vendor_record_id:
            same_type_policies = get_existing_policies_by_vendor_and_type(
                policies_table, vendor_record_id, policy_type
            )
            if same_type_policies and new_effective:
                # Check if this is a renewal (newer) or a duplicate (older/same)
                is_duplicate = False
                for existing in same_type_policies:
                    existing_effective_raw = existing["fields"].get("Effective Date", "")
                    existing_effective = _parse_date_safe(existing_effective_raw)
                    if existing_effective and new_effective <= existing_effective:
                        logger.info(
                            "Policy %d (%s) effective %s is not newer than existing %s (ID: %s) — skipping as duplicate.",
                            idx, policy_type, effective_raw, existing_effective_raw, existing["id"],
                        )
                        touched_ids.append(existing["id"])
                        is_duplicate = True
                        break
                if is_duplicate:
                    continue

                # New policy is newer — mark all existing same-type policies as Superseded
                for existing in same_type_policies:
                    try:
                        policies_table.update(existing["id"], {"Status": "Superseded"}, typecast=True)
                        logger.info(
                            "Marked policy %s (%s %s) as Superseded — replaced by renewal.",
                            existing["id"],
                            policy_type,
                            existing["fields"].get("Policy Number", ""),
                        )
                    except Exception as exc:
                        logger.warning("Failed to mark policy %s as Superseded: %s", existing["id"], exc)

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

        # Claims-made fields from extraction
        policy_basis = (policy.get("policy_basis") or "").strip().lower()
        if policy_basis in ("claims-made", "occurrence", "unknown"):
            fields["fldPMR5nVSExiH2h2"] = policy_basis  # Policy Basis
        retro_date = (policy.get("retro_date") or "").strip()
        if retro_date:
            fields["fldHSPP6979xM7OC6"] = retro_date  # Retroactive Date
        tail_coverage = policy.get("tail_coverage_evidenced")
        if tail_coverage is True:
            fields["fldZz5vGdz9vDaxz0"] = True  # Tail Coverage Evidenced

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

        record = policies_table.create(fields, typecast=True)
        new_records_created += 1
        logger.info(
            "Created Insurance Policy — ID: %s  Policy #: %s",
            record["id"], policy_number,
        )
        touched_ids.append(record["id"])

        # ── Carrier continuity check on renewals ─────────────────────────
        # same_type_policies is set above in the renewal detection block.
        # If it exists and had records, this is a renewal — run the check.
        if 'same_type_policies' in dir() and same_type_policies:
            try:
                # Pick the most recent superseded policy for comparison
                superseded = sorted(
                    same_type_policies,
                    key=lambda p: p["fields"].get("Expiration Date") or "",
                    reverse=True,
                )[0]
                continuity = check_carrier_continuity(policy, superseded)

                if continuity["carrier_changed"]:
                    # Write flag to new policy record
                    try:
                        policies_table.update(record["id"], {
                            "fldeit6q3YlC68bDI": (  # Endorsement Flags
                                f"CARRIER_CHANGED: Certificate documentation shows carrier changed "
                                f"on {continuity['policy_type']} from '{continuity['old_carrier']}' "
                                f"to '{continuity['new_carrier']}'."
                            ),
                        }, typecast=True)
                    except Exception:
                        pass
                    logger.info(
                        "Carrier change detected on renewal: %s → %s (%s)",
                        continuity["old_carrier"], continuity["new_carrier"],
                        continuity["policy_type"],
                    )

                if continuity["policy_number_changed"]:
                    logger.info(
                        "Policy number changed on renewal: %s → %s (%s)",
                        continuity["old_number"], continuity["new_number"],
                        continuity["policy_type"],
                    )
            except Exception as exc:
                logger.warning("Carrier continuity check failed: %s", exc)

    # ── Stamp extraction as Duplicate if all policies matched existing records ─
    if policies and new_records_created == 0 and touched_ids and incoming_table and extraction_id:
        # Every policy was either an existing-by-number refresh or an effective-date duplicate
        duplicate_of_id = touched_ids[0] if touched_ids else ""
        try:
            incoming_table.update(extraction_id, {
                "Processing Status": "Duplicate",
                "fldYmU49onpvXabJi": f"{duplicate_of_id}",  # Duplicate Of
            }, typecast=True)
            logger.info(
                "All %d policies matched existing records — extraction %s stamped as Duplicate (of %s)",
                len(policies), extraction_id, duplicate_of_id,
            )
        except Exception as exc:
            logger.warning("Failed to stamp extraction as Duplicate: %s", exc)

    return touched_ids


def upload_pdf_attachment(pdf_path: Path, record_id: str, certs_table) -> bool:
    """
    Upload a local PDF file to the Certificate File attachment field in Airtable.
    Uses pyairtable's built-in upload_attachment method.

    Returns True on success, False on failure (non-fatal — cert still gets created).
    """
    if not pdf_path.exists():
        logger.warning("Cannot upload PDF — file not found: %s", pdf_path)
        return False

    try:
        certs_table.upload_attachment(record_id, "Certificate File", str(pdf_path))
        logger.info(
            "Uploaded PDF attachment to cert %s — file: %s",
            record_id, pdf_path.name,
        )
        return True
    except Exception as exc:
        logger.warning("PDF upload failed for cert %s: %s", record_id, exc)
        return False


def resolve_pdf_path(source_filename: str) -> Optional[Path]:
    """
    Given a source_filename (e.g. 'hales.json'), find the original PDF
    in the uploads/ directory. Tries exact stem match first, then
    falls back to the incoming_pdfs/ directory.
    """
    if not source_filename:
        return None

    stem = Path(source_filename).stem  # 'hales.json' → 'hales'
    upload_dir = Path(config.UPLOAD_DIR)
    incoming_dir = Path(__file__).parent / "incoming_pdfs"

    for directory in (upload_dir, incoming_dir):
        if not directory.exists():
            continue
        for ext in (".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif"):
            candidate = directory / f"{stem}{ext}"
            if candidate.exists():
                return candidate

    logger.debug("No PDF found for source_filename '%s'", source_filename)
    return None


def create_certificate(
    certs_table,
    vendor_record_id: str,
    named_insured: str,
    source_filename: str,
    certificate_date: str,
    policy_record_ids: list,
    matched_request_id: Optional[str] = None,
    matched_client_id: Optional[str] = None,
    base_id: Optional[str] = None,
) -> dict:
    """Create one Insurance Certificate record linked to the Vendor.
    If the original PDF is found on disk, uploads it to the Certificate File
    attachment field.
    """
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

    # ── Upload the source PDF as an attachment ──
    pdf_path = resolve_pdf_path(source_filename)
    if pdf_path:
        upload_pdf_attachment(pdf_path, record["id"], certs_table)
    else:
        logger.info("No PDF file found for '%s' — Certificate File left empty", source_filename)

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

    # ── Step 1: fetch all pending Imported extractions ──────────────────────────
    logger.info("Step 1 — Fetching all 'Imported' records from '%s'...", TABLE_INCOMING)
    pending_extractions = fetch_all_pending_extractions(tables[TABLE_INCOMING])

    if not pending_extractions:
        logger.info("No records with Processing Status = 'Imported' found. Nothing to do.")
        return

    total = len(pending_extractions)
    succeeded = 0
    failed_ids = []
    logger.info("Found %d pending extraction(s) to process", total)

    for extract_idx, extraction in enumerate(pending_extractions, start=1):
        extraction_id     = extraction["id"]
        extraction_fields = extraction["fields"]
        named_insured_preview = (extraction_fields.get("Named Insured") or extraction_fields.get("Source Filename") or "")[:50]
        logger.info("Processing extraction %d of %d: %s (ID: %s)",
                     extract_idx, total, named_insured_preview, extraction_id)
        try:
            _process_single_extraction(extraction, tables, base_id=base_id)
            succeeded += 1
        except Exception as exc:
            logger.error(
                "Processor failed on extraction %d of %d (ID: %s): %s",
                extract_idx, total, extraction_id, exc,
            )
            failed_ids.append(extraction_id)

    logger.info(
        "=== Module 4 complete: %d of %d processed successfully%s ===",
        succeeded, total,
        f" (failed: {', '.join(failed_ids)})" if failed_ids else "",
    )


def _process_single_extraction(extraction, tables, base_id=""):
    """Process a single Incoming Extraction record through the full pipeline."""
    extraction_id     = extraction["id"]
    extraction_fields = extraction["fields"]
    vendor = None

    named_insured     = (extraction_fields.get("Named Insured") or "").strip()
    source_filename   = (extraction_fields.get("Source Filename") or "").strip()
    raw_json_str      = extraction_fields.get("Raw JSON") or ""

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

            # Try to match an existing vendor before auto-creating
            vendor = find_vendor(tables[TABLE_VENDORS], named_insured)
            if vendor:
                logger.info(
                    "Pending Match resolved to existing vendor — ID: %s  Name: '%s'",
                    vendor["id"],
                    vendor["fields"].get("Vendor Name", ""),
                )
            else:
                if named_insured_matches_client(tables[TABLE_CLIENTS], named_insured):
                    set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
                    logger.info("=== Module 4 complete (named insured matches client — needs manual review) ===")
                    return

                vendor = create_vendor(tables[TABLE_VENDORS], named_insured)
                logger.info(
                    "Pending Match vendor handling complete — vendor auto-created and processing will continue."
                )
    elif match_evaluation.match_status == "Unmatched":
        logger.info(
            "Match status is Unmatched — flagging for manual review. Named insured: '%s'",
            named_insured,
        )
        set_processing_status(tables[TABLE_INCOMING], extraction_id, "Unmatched")
        logger.info("=== Module 4 complete (unmatched — queued for manual review) ===")
        return
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

            if named_insured_matches_client(tables[TABLE_CLIENTS], named_insured):
                set_processing_status(tables[TABLE_INCOMING], extraction_id, "Needs Review")
                logger.info("=== Module 4 complete (named insured matches client — needs manual review) ===")
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
        base_id=base_id,
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
        incoming_table=tables[TABLE_INCOMING],
        extraction_id=extraction_id,
        certs_table=tables[TABLE_CERTS],
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

    # ── Step 5b: Timing evaluation ──────────────────────────────────────────
    try:
        from policy_timing_evaluator import (
            evaluate_certificate_timing,
            write_timing_to_policy,
            write_timing_to_certificate,
        )
        # Build list of new policy dicts from the raw data + touched IDs
        new_policy_dicts = []
        for i, policy in enumerate(policies):
            if i < len(touched_policy_ids) and touched_policy_ids[i]:
                new_policy_dicts.append({
                    "id": touched_policy_ids[i],
                    "fields": {
                        "Policy Type": normalize_policy_type((policy.get("policy_type") or "").strip()),
                        "Effective Date": (policy.get("effective_date") or "").strip(),
                        "Expiration Date": (policy.get("expiration_date") or "").strip(),
                        "Status": compute_policy_status(
                            (policy.get("expiration_date") or "").strip(),
                            (policy.get("policy_number") or "").strip(),
                        ),
                    },
                })

        # Fetch all existing policies for this vendor
        all_vendor_policies = tables[TABLE_POLICIES].all(
            formula=f"FIND('{vendor_id}', ARRAYJOIN({{Vendor Link}}))"
        )

        timing_result = evaluate_certificate_timing(new_policy_dicts, all_vendor_policies)

        if timing_result.findings:
            logger.info("Timing evaluation: %d findings for certificate %s",
                        len(timing_result.findings), certificate_id)
            for f in timing_result.findings:
                logger.info("  [%s] %s: %s", f.severity.upper(), f.flag, f.message)

            # Write timing flags to each new policy record
            for pd in new_policy_dicts:
                write_timing_to_policy(tables[TABLE_POLICIES], pd["id"], timing_result)

            # Append timing summary to certificate
            write_timing_to_certificate(tables[TABLE_CERTS], certificate_id, timing_result)
        else:
            logger.info("Timing evaluation: no issues for certificate %s", certificate_id)
    except Exception as exc:
        logger.warning("Timing evaluation failed (non-fatal): %s", exc)

    # ── Step 5c: Mid-term change detection ──────────────────────────────────
    try:
        from midterm_change_detector import detect_midterm_changes, write_midterm_changes_to_certificate

        # Fetch prior policies for this vendor (excluding ones just created)
        prior_policies = [
            p for p in all_vendor_policies
            if p.get("id") not in touched_policy_ids
        ] if 'all_vendor_policies' in dir() else []

        if not prior_policies and vendor_id:
            prior_policies = tables[TABLE_POLICIES].all(
                formula=f"FIND('{vendor_id}', ARRAYJOIN({{Vendor Link}}))"
            )
            prior_policies = [p for p in prior_policies if p.get("id") not in touched_policy_ids]

        if prior_policies:
            # Build new policy dicts from raw extraction data
            new_policy_dicts = []
            for i, policy in enumerate(policies):
                if i < len(touched_policy_ids) and touched_policy_ids[i]:
                    new_policy_dicts.append({
                        "fields": {
                            "Policy Type": normalize_policy_type((policy.get("policy_type") or "").strip()),
                            "Carrier": (policy.get("carrier") or "").strip(),
                            "Coverage Limits": (policy.get("coverage_limits") or "").strip(),
                            "Additional Insured": policy.get("additional_insured_checked", False),
                            "Waiver": policy.get("waiver_of_subrogation_checked", False),
                            "Primary Noncontributory": policy.get("primary_noncontributory_checked", False),
                        },
                    })

            # Get prior named insured from the most recent prior certificate
            prior_named_insured = ""
            try:
                vendor_certs = tables[TABLE_CERTS].all(
                    formula=f"FIND('{vendor_id}', ARRAYJOIN({{Vendor Link}}))"
                )
                vendor_certs = [c for c in vendor_certs if c.get("id") != certificate_id]
                if vendor_certs:
                    vendor_certs.sort(key=lambda c: c.get("createdTime", ""), reverse=True)
                    prior_named_insured = vendor_certs[0].get("fields", {}).get("Named Insured", "")
            except Exception:
                pass

            midterm_result = detect_midterm_changes(
                new_policies=new_policy_dicts,
                prior_policies=prior_policies,
                new_named_insured=named_insured,
                prior_named_insured=prior_named_insured,
            )

            if midterm_result.findings:
                logger.info("Mid-term changes detected for certificate %s: %d findings",
                            certificate_id, len(midterm_result.findings))
                for f in midterm_result.findings:
                    logger.info("  [%s] %s: %s", f.severity.upper(), f.reason_code, f.message)
                write_midterm_changes_to_certificate(tables[TABLE_CERTS], certificate_id, midterm_result)
            else:
                logger.info("No mid-term changes detected for certificate %s", certificate_id)
        else:
            logger.info("No prior policies for mid-term comparison — skipping")
    except Exception as exc:
        logger.warning("Mid-term change detection failed (non-fatal): %s", exc)

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

        # ── Resolve client via assignments if matcher didn't find one ──────
        if not matched_client_id and vendor_id:
            try:
                m7b_assignments_lookup = fetch_active_assignments_for_vendor(
                    vendor_id, tables[TABLE_ASSIGNMENTS]
                )
                if m7b_assignments_lookup:
                    resolved_client = (m7b_assignments_lookup[0]["fields"].get("Client Link") or [None])[0]
                    if resolved_client:
                        matched_client_id = resolved_client
                        logger.info(
                            "Client resolved via Vendor Client Assignments: %s",
                            matched_client_id,
                        )
            except Exception as exc:
                logger.warning("Assignment-based client resolution failed: %s", exc)

        # ── Module 7b full per-client requirement check ──────────────────────
        if matched_client_id:
            m7b_client_reqs = fetch_requirements_for_client(
                matched_client_id, tables[TABLE_REQUIREMENTS]
            )
            m7b_vendor_policies = fetch_policies_for_vendor(
                vendor_id, tables[TABLE_POLICIES]
            )
            m7b_assignments = fetch_active_assignments_for_vendor(
                vendor_id, tables[TABLE_ASSIGNMENTS]
            )
            # Find the assignment for this specific client
            m7b_assignment = next(
                (a for a in m7b_assignments
                 if (a["fields"].get("Client Link") or [None])[0] == matched_client_id),
                None,
            )
            if m7b_assignment and m7b_client_reqs:
                # Resolve certificate holder names for holder comparison
                _coi_holder = raw_data.get("certificate_holder") or ""
                _expected_holder = ""
                try:
                    _client_rec = tables[TABLE_CLIENTS].get(matched_client_id)
                    _expected_holder = _client_rec.get("fields", {}).get("Certificate Holder") or ""
                except Exception:
                    pass

                m7b_status, m7b_failure_reasons = evaluate_assignment(
                    vendor, m7b_client_reqs, m7b_vendor_policies,
                    m7b_assignment, tables[TABLE_ASSIGNMENTS],
                    certificate_holder_on_coi=_coi_holder,
                    expected_certificate_holder=_expected_holder,
                )
                logger.info(
                    "Module 7b live compliance check — vendor=%s client=%s status=%s reasons=%s",
                    vendor_name, matched_client_id, m7b_status, m7b_failure_reasons,
                )
                compliance_result = ReturnedCoiComplianceResult(
                    outcome=m7b_status,
                    decision_summary="; ".join(m7b_failure_reasons) if m7b_failure_reasons else "All requirements satisfied.",
                    failure_reasons=[
                        ComplianceFailureReason(
                            code="REQUIREMENT_FAILURE",
                            message=reason,
                            severity="error",
                        )
                        for reason in m7b_failure_reasons
                    ],
                    evaluator_version="v2-module-7b",
                    should_queue_deficiency_email=(m7b_status == "Has Open Items"),
                    metadata={
                        "stub": False,
                        "rule_version": "v2-module-7b",
                        "required_policy_types": sorted({r["fields"].get("Policy Type", "") for r in m7b_client_reqs}),
                        "requirement_source": "module_7b_requirement_validator",
                    },
                )
            else:
                logger.info(
                    "Module 7b skipped — no matching assignment or no client requirements. "
                    "assignment_found=%s client_reqs_count=%d",
                    m7b_assignment is not None, len(m7b_client_reqs),
                )
                compliance_result = ReturnedCoiComplianceResult(
                    outcome="Needs Review",
                    decision_summary="No active assignment found for this vendor/client pair, or no client requirements configured.",
                    failure_reasons=[],
                    evaluator_version="v2-module-7b",
                    metadata={"stub": False, "rule_version": "v2-module-7b-fallback",
                              "required_policy_types": [], "requirement_source": "none"},
                )
        else:
            logger.info("Module 7b skipped — no matched client for compliance check.")
            compliance_result = ReturnedCoiComplianceResult(
                outcome="Needs Review",
                decision_summary="No client matched — cannot run per-client requirement check.",
                failure_reasons=[],
                evaluator_version="v2-module-7b",
                metadata={"stub": False, "rule_version": "v2-module-7b-no-client",
                          "required_policy_types": [], "requirement_source": "none"},
            )
        evaluator_required_policy_types = compliance_result.metadata.get("required_policy_types", [])
        evaluator_requirement_source = compliance_result.metadata.get("requirement_source", "none")
        logger.info(
            "Compliance evaluator requirement context — matched_client_id=%s required_policy_types=%s requirement_source=%s",
            matched_client_id,
            evaluator_required_policy_types,
            evaluator_requirement_source,
        )

        compliance_evaluated_at_utc_dt = datetime.now(timezone.utc).replace(microsecond=0)
        compliance_evaluated_at_utc = compliance_evaluated_at_utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        compliance_evaluated_at_et = (
            compliance_evaluated_at_utc_dt
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
        tables[TABLE_CERTS].update(certificate_id, compliance_update_fields, typecast=True)
        logger.info(
            "Certificate %s compliance stub result written to Insurance Certificates.",
            certificate_id,
        )

        if compliance_result.outcome == "Has Open Items":
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

    logger.info("=== Extraction %s processed ===", extraction_id)


if __name__ == "__main__":
    try:
        run()
    except (EnvironmentError, ValueError, RuntimeError) as exc:
        logger.error("Processor failed: %s", exc)
        raise SystemExit(1) from exc