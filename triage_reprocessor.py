"""
Triage Reprocessor — Carolina Compliance Solutions

Re-triggers the compliance evaluation pipeline on extraction records
that have been manually reviewed and corrected in Airtable.

Triggered two ways:
- Automatically: daily cron checks for records where Manually Reviewed By is set
  and Processing Status is still In Review
- Manually: python run_pipeline.py --reprocess <record_id>
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Airtable field IDs ───────────────────────────────────────────────────────

# Incoming Extractions
FLD_PROC_STATUS       = "fld4KqSQEX32Zenut"
FLD_NAMED_INSURED     = "fld4X90MLBQIqTNTn"
FLD_MATCHED_VENDOR    = "fldOku9CPphnETmTA"
FLD_RAW_JSON          = "fldhXhICjoAUrP8Mp"
FLD_MANUALLY_REVIEWED = "fldHtC3m5NNyl60lC"
FLD_REPROCESS_AT      = "flds8TE4N8DNBHgVy"
FLD_MATCH_STATUS      = "fldLuIpTPVQvyLlqT"

# Insurance Certificates
FLD_CERT_COMP_STATUS  = "fldvy3UFvZq1D57tn"
FLD_CERT_COMP_SUMMARY = "fldX7L6Goy7LomDMo"

TABLE_EXTRACTIONS     = "tblT88Ty6d6M766oY"
TABLE_VENDORS         = "tblsOphSd5DKSZEro"
TABLE_CLIENT_REQS     = "tblFGQ6XgOIHSWtQN"
TABLE_POLICIES        = "tblpPcmm5ANE0bMNB"
TABLE_ASSIGNMENTS     = "tblpYKywfs0YHiQ98"
TABLE_CERTS           = "tbl0IH6zQQsXBff3l"
TABLE_OVERRIDES       = "tblKVIdTYwF1Sp5Iw"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_linked_id(value) -> Optional[str]:
    """Extract first ID from an Airtable linked record field."""
    if isinstance(value, list) and value:
        return value[0] if isinstance(value[0], str) else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def validate_extraction_for_reprocess(fields: Dict[str, Any]) -> tuple:
    """Validate that an extraction record has the minimum fields needed.

    Returns (is_valid, missing_fields_list).
    """
    missing = []

    named_insured = (fields.get("Named Insured") or fields.get(FLD_NAMED_INSURED) or "").strip()
    if not named_insured:
        missing.append("Named Insured")

    vendor_id = _parse_linked_id(
        fields.get("Matched Vendor") or fields.get(FLD_MATCHED_VENDOR)
    )
    if not vendor_id:
        missing.append("Matched Vendor")

    # Check raw JSON for at least one policy with type + expiration
    raw_json_str = fields.get("Raw JSON") or fields.get(FLD_RAW_JSON) or ""
    has_valid_policy = False
    try:
        raw_data = json.loads(raw_json_str) if raw_json_str else {}
        for policy in raw_data.get("policies", []):
            pt = (policy.get("policy_type") or "").strip()
            exp = (policy.get("expiration_date") or "").strip()
            if pt and exp:
                has_valid_policy = True
                break
    except (json.JSONDecodeError, TypeError):
        pass

    if not has_valid_policy:
        missing.append("At least one policy with policy_type and expiration_date")

    return (len(missing) == 0, missing)


def reprocess_from_triage(extraction_record_id: str, api) -> bool:
    """Re-trigger compliance evaluation on a manually reviewed extraction record.

    Args:
        extraction_record_id: Airtable record ID of the Incoming Extraction
        api: pyairtable Api instance

    Returns:
        True if reprocessed successfully, False if still invalid.
    """
    import config

    extractions_table = api.table(config.AIRTABLE_BASE_ID, TABLE_EXTRACTIONS)

    # Fetch the record
    try:
        record = extractions_table.get(extraction_record_id)
    except Exception as exc:
        logger.error("Failed to fetch extraction %s: %s", extraction_record_id, exc)
        return False

    fields = record.get("fields", {})
    current_status = fields.get("Processing Status") or fields.get(FLD_PROC_STATUS) or ""

    # Idempotency: if already Reprocessed, skip
    if current_status == "Reprocessed":
        logger.info("Extraction %s already reprocessed — skipping", extraction_record_id)
        return True

    # Validate required fields
    is_valid, missing = validate_extraction_for_reprocess(fields)

    if not is_valid:
        logger.info(
            "Extraction %s still invalid — missing: %s. Leaving in review.",
            extraction_record_id, ", ".join(missing),
        )
        # Reset escalation clock by updating status to In Review
        try:
            extractions_table.update(extraction_record_id, {
                FLD_PROC_STATUS: "In Review",
            }, typecast=True)
        except Exception as exc:
            logger.error("Failed to reset status on %s: %s", extraction_record_id, exc)
        return False

    # ── Valid — run compliance evaluation ─────────────────────────────────
    vendor_id = _parse_linked_id(
        fields.get("Matched Vendor") or fields.get(FLD_MATCHED_VENDOR)
    )

    logger.info("Reprocessing extraction %s for vendor %s", extraction_record_id, vendor_id)

    try:
        import module_7b_requirement_validator as m7b

        vendors_table = api.table(config.AIRTABLE_BASE_ID, TABLE_VENDORS)
        client_reqs_table = api.table(config.AIRTABLE_BASE_ID, TABLE_CLIENT_REQS)
        policies_table = api.table(config.AIRTABLE_BASE_ID, TABLE_POLICIES)
        assignments_table = api.table(config.AIRTABLE_BASE_ID, TABLE_ASSIGNMENTS)
        overrides_table = api.table(config.AIRTABLE_BASE_ID, TABLE_OVERRIDES)

        # Fetch vendor
        vendor = vendors_table.get(vendor_id)
        vendor_name = vendor["fields"].get("Vendor Name", vendor_id)

        # Find active assignments
        active_assignments = m7b.fetch_active_assignments_for_vendor(vendor_id, assignments_table)

        if active_assignments:
            for assignment in active_assignments:
                client_id = _parse_linked_id(assignment["fields"].get("Client Link"))
                if not client_id:
                    continue

                client_reqs = m7b.fetch_requirements_for_client(client_id, client_reqs_table)
                if not client_reqs:
                    continue

                vendor_policies = m7b.fetch_policies_for_vendor(vendor_id, policies_table)

                new_decision, failure_reasons = m7b.evaluate_assignment(
                    vendor, client_reqs, vendor_policies, assignment, assignments_table,
                    insurance_policies_table=policies_table,
                    overrides_table=overrides_table, client_id=client_id,
                )

                logger.info(
                    "Reprocess compliance result: vendor=%s status=%s reasons=%d",
                    vendor_name, new_decision, len(failure_reasons),
                )
        else:
            logger.info("No active assignments for vendor %s — legacy compliance check", vendor_name)

        # Update linked certificate if exists
        cert_links = fields.get("Insurance Certificates") or []
        if cert_links:
            cert_id = _parse_linked_id(cert_links)
            if cert_id:
                try:
                    certs_table = api.table(config.AIRTABLE_BASE_ID, TABLE_CERTS)
                    certs_table.update(cert_id, {
                        FLD_CERT_COMP_STATUS: "Reprocessed",
                        FLD_CERT_COMP_SUMMARY: (
                            f"Record reprocessed after manual review on {_utc_now_iso()}."
                        ),
                    }, typecast=True)
                except Exception as exc:
                    logger.warning("Failed to update certificate %s: %s", cert_id, exc)

    except Exception as exc:
        logger.error("Compliance evaluation failed during reprocess of %s: %s",
                     extraction_record_id, exc)
        return False

    # Mark as Reprocessed + write timestamp
    try:
        extractions_table.update(extraction_record_id, {
            FLD_PROC_STATUS: "Reprocessed",
            FLD_REPROCESS_AT: _utc_now_iso(),
        }, typecast=True)
        logger.info("Extraction %s reprocessed successfully", extraction_record_id)
    except Exception as exc:
        logger.error("Failed to set Reprocessed status on %s: %s", extraction_record_id, exc)

    return True


def check_reviewed_for_reprocess(api):
    """Daily cron: find records where Manually Reviewed By is set and
    Processing Status is In Review. Reprocess each one.
    """
    import config

    extractions_table = api.table(config.AIRTABLE_BASE_ID, TABLE_EXTRACTIONS)

    try:
        formula = (
            f"AND("
            f"{{{FLD_MANUALLY_REVIEWED}}} != '', "
            f"OR("
            f"{{{FLD_PROC_STATUS}}} = 'In Review', "
            f"{{{FLD_PROC_STATUS}}} = 'Low Confidence', "
            f"{{{FLD_PROC_STATUS}}} = 'Unmatched'"
            f")"
            f")"
        )
        records = extractions_table.all(formula=formula)
    except Exception as exc:
        logger.error("Failed to fetch reviewed records: %s", exc)
        return

    if not records:
        logger.info("No reviewed records pending reprocess")
        return

    logger.info("Found %d reviewed records to reprocess", len(records))
    succeeded = 0
    still_invalid = 0

    for record in records:
        result = reprocess_from_triage(record["id"], api)
        if result:
            succeeded += 1
        else:
            still_invalid += 1

    logger.info(
        "Reprocess check complete: %d succeeded, %d still invalid",
        succeeded, still_invalid,
    )
