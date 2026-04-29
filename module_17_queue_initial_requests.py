import logging
from typing import Any, Dict, List, Optional, Tuple

from pyairtable import Api

import config


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


from airtable_constants import (
    TBL_VENDORS, TBL_EMAIL_QUEUE, TBL_CLIENTS, TBL_VENDOR_CLIENT_ASSIGNMENTS,
    V_VENDOR_NAME, V_VENDOR_EMAIL, V_SEND_REQUEST,
    EQ_PRIMARY_EMAIL, EQ_SUBJECT, EQ_BODY, EQ_EMAIL_TYPE, EQ_EMAIL_STATUS,
    EQ_RECORD_STATUS, EQ_VENDOR_LINK, EQ_CLIENT_LINK, EQ_REMINDER_STATUS, EQ_SEND_AFTER,
    A_VENDOR_LINK, A_CLIENT_LINK, A_ACTIVE,
    C_CLIENT_NAME, C_CLIENT_LINK,
    STATUS_PENDING, STATUS_ACTIVE, STATUS_SENT,
    EMAIL_TYPE_INITIAL_REQUEST,
)

VENDORS_TABLE_NAME = TBL_VENDORS
EMAIL_QUEUE_TABLE_NAME = TBL_EMAIL_QUEUE
CLIENTS_TABLE_NAME = TBL_CLIENTS
VENDOR_CLIENT_ASSIGNMENTS_TABLE_NAME = TBL_VENDOR_CLIENT_ASSIGNMENTS

VENDOR_NAME_FIELD = V_VENDOR_NAME
VENDOR_EMAIL_FIELD = V_VENDOR_EMAIL
SEND_REQUEST_FIELD = V_SEND_REQUEST

QUEUE_PRIMARY_EMAIL_FIELD = EQ_PRIMARY_EMAIL
QUEUE_SUBJECT_FIELD = EQ_SUBJECT
QUEUE_BODY_FIELD = EQ_BODY
QUEUE_EMAIL_TYPE_FIELD = EQ_EMAIL_TYPE
QUEUE_EMAIL_STATUS_FIELD = EQ_EMAIL_STATUS
QUEUE_RECORD_STATUS_FIELD = EQ_RECORD_STATUS
QUEUE_VENDOR_LINK_FIELD = EQ_VENDOR_LINK
QUEUE_CLIENT_LINK_FIELD = EQ_CLIENT_LINK

VENDOR_CLIENT_LINK_FIELD = C_CLIENT_LINK
ASSIGNMENT_VENDOR_LINK_FIELD = A_VENDOR_LINK
ASSIGNMENT_CLIENT_LINK_FIELD = A_CLIENT_LINK
ASSIGNMENT_ACTIVE_FIELD = A_ACTIVE
CLIENT_NAME_FIELD = C_CLIENT_NAME
CLIENT_PLACEHOLDER = "[Client Name]"

INITIAL_REQUEST_TYPE = "Initial Request"
PENDING_STATUS = "Pending"
ACTIVE_STATUS = "Active"
SENT_STATUS = "Sent"

QUEUE_REMINDER_STATUS_FIELD = "Reminder Status"
QUEUE_SEND_AFTER_FIELD = "Send After"


def _require_airtable_config() -> None:
    if not config.AIRTABLE_API_KEY or not config.AIRTABLE_BASE_ID:
        raise RuntimeError(
            "Missing required Airtable configuration: AIRTABLE_API_KEY and AIRTABLE_BASE_ID"
        )


def _escape_formula_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _extract_first_linked_id(value: Any) -> Optional[str]:
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, str) and first.strip():
            return first.strip()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _fetch_client_name_by_id(clients_table, client_id: str) -> Optional[str]:
    try:
        client_record = clients_table.get(client_id)
    except Exception:
        return None
    fields = client_record.get("fields", {})
    client_name = (fields.get(CLIENT_NAME_FIELD) or "").strip()
    return client_name or None


def _resolve_client(
    vendor_id: str,
    vendor_name: str,
    vendor_fields: Dict[str, Any],
    clients_table,
    assignments_table,
) -> Tuple[str, Optional[str]]:
    """Return (client_name, client_id) for the vendor's active assignment, or
    (CLIENT_PLACEHOLDER, None) if no client could be resolved. The client_id
    is required so the queue row can populate its Client link field — without
    it, module_18's _client_requirements_received guard short-circuits to False."""
    try:
        vendor_name_clean = vendor_name.strip()
        escaped_vendor_name = _escape_formula_value(vendor_name_clean)
        assignment_formula = (
            "AND("
            f"{{{ASSIGNMENT_ACTIVE_FIELD}}}=1,"
            f"FIND('{escaped_vendor_name}', ARRAYJOIN({{Vendor Link}}))"
            ")"
        )
        assignments = assignments_table.all(formula=assignment_formula, max_records=1)
        if assignments:
            logger.info(
                "client name resolution: assignment match found for vendor %s",
                vendor_name,
            )
            assignment_fields = assignments[0].get("fields", {})
            assigned_client_id = _extract_first_linked_id(assignment_fields.get(ASSIGNMENT_CLIENT_LINK_FIELD))
            if assigned_client_id:
                assigned_client_name = _fetch_client_name_by_id(clients_table, assigned_client_id)
                if assigned_client_name:
                    return assigned_client_name, assigned_client_id
    except Exception:
        pass

    legacy_client_id = _extract_first_linked_id(vendor_fields.get(VENDOR_CLIENT_LINK_FIELD))
    if legacy_client_id:
        legacy_client_name = _fetch_client_name_by_id(clients_table, legacy_client_id)
        if legacy_client_name:
            return legacy_client_name, legacy_client_id

    return CLIENT_PLACEHOLDER, None


def _build_subject(client_name: str) -> str:
    return f"Action Needed: COI for {client_name}"


def _build_body(vendor_name: str, client_name: str) -> str:
    safe_vendor_name = vendor_name or "Vendor"
    return (
        f"Hi {safe_vendor_name},\n\n"
        f"I’m with Carolina Compliance Solutions, helping manage insurance compliance for {client_name}.\n\n"
        f"Please send your current Certificate of Insurance and make sure {client_name} is listed as the certificate holder.\n\n"
        "If you have any questions, feel free to reply directly to this email.\n\n"
        "Thank you,\n"
        "Carolina Compliance Solutions"
    )


def _fetch_vendors_ready_for_initial_request(vendors_table) -> List[Dict[str, Any]]:
    formula = f"{{{SEND_REQUEST_FIELD}}}=1"
    return vendors_table.all(formula=formula)


def _find_active_unsent_initial_request_record_id(
    email_queue_table,
    vendor_name: str,
) -> Optional[str]:
    """Check for an existing active unsent Initial Request for this vendor.

    Uses vendor display name (not record ID) because ARRAYJOIN on linked
    record fields returns display names in Airtable formulas.
    """
    safe_vendor_name = _escape_formula_value(vendor_name)
    formula = f"AND({{Email Type}}='Initial Request', {{Reminder Status}}='Queued', FIND('{safe_vendor_name}', ARRAYJOIN({{Vendor}})))"
    records = email_queue_table.all(formula=formula)
    return records[0]["id"] if records else None


def _create_queue_record(
    email_queue_table,
    vendor_id: str,
    vendor_name: str,
    vendor_email: str,
    client_name: str,
    client_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    # Guard: do not create orphaned queue records with no vendor
    if not vendor_id or not vendor_id.strip():
        logger.warning("Skipped queue record — no vendor ID for '%s'", vendor_name)
        return None

    from datetime import datetime, timezone
    send_after = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    fields = {
        QUEUE_PRIMARY_EMAIL_FIELD: vendor_email,
        QUEUE_SUBJECT_FIELD: _build_subject(client_name),
        QUEUE_BODY_FIELD: _build_body(vendor_name, client_name),
        QUEUE_EMAIL_TYPE_FIELD: INITIAL_REQUEST_TYPE,
        QUEUE_EMAIL_STATUS_FIELD: PENDING_STATUS,
        QUEUE_RECORD_STATUS_FIELD: ACTIVE_STATUS,
        QUEUE_REMINDER_STATUS_FIELD: "Queued",
        QUEUE_SEND_AFTER_FIELD: send_after,
        QUEUE_VENDOR_LINK_FIELD: [vendor_id],
    }
    # Client link is required for module_18's requirements-confirmed guard.
    # Without it, _client_requirements_received(api, None) short-circuits
    # to False and the Initial Request is silently skipped.
    if client_id:
        fields[QUEUE_CLIENT_LINK_FIELD] = [client_id]
    else:
        logger.warning(
            "Queue record for vendor %s (%s) created without Client link — "
            "module_18 will skip the send because client requirements can't be checked",
            vendor_name, vendor_id,
        )
    return email_queue_table.create(fields)


def _reset_vendor_trigger(vendors_table, vendor_id: str) -> None:
    vendors_table.update(vendor_id, {SEND_REQUEST_FIELD: False})


def run() -> None:
    logger.info("module_17_queue_initial_requests: start")

    try:
        _require_airtable_config()
        api = Api(config.AIRTABLE_API_KEY)
        vendors_table = api.table(config.AIRTABLE_BASE_ID, VENDORS_TABLE_NAME)
        email_queue_table = api.table(config.AIRTABLE_BASE_ID, EMAIL_QUEUE_TABLE_NAME)
        clients_table = api.table(config.AIRTABLE_BASE_ID, CLIENTS_TABLE_NAME)
        assignments_table = api.table(config.AIRTABLE_BASE_ID, VENDOR_CLIENT_ASSIGNMENTS_TABLE_NAME)
    except Exception as exc:
        logger.error("Airtable error during setup: %s", exc)
        return

    try:
        vendors = _fetch_vendors_ready_for_initial_request(vendors_table)
    except Exception as exc:
        logger.error("Airtable error while loading vendors: %s", exc)
        return

    logger.info("Vendors with Send Request checked: %d", len(vendors))

    for vendor in vendors:
        vendor_id = vendor.get("id", "")
        fields = vendor.get("fields", {})
        vendor_name = fields.get(VENDOR_NAME_FIELD, "Unknown Vendor")
        vendor_email = (fields.get(VENDOR_EMAIL_FIELD) or "").strip()

        logger.info("vendor found: %s (%s)", vendor_name, vendor_id)

        if not vendor_email:
            logger.warning("skipped missing email: %s (%s)", vendor_name, vendor_id)
            continue

        client_name, client_id = _resolve_client(
            vendor_id=vendor_id,
            vendor_name=vendor_name,
            vendor_fields=fields,
            clients_table=clients_table,
            assignments_table=assignments_table,
        )
        if not client_name:
            client_name = CLIENT_PLACEHOLDER

        if client_name == CLIENT_PLACEHOLDER:
            logger.info(
                "client name resolution: fallback placeholder used for vendor %s (no active assignment found)",
                vendor_name,
            )
        else:
            logger.info(
                "client name resolution: resolved '%s' for vendor %s",
                client_name,
                vendor_name,
            )

        try:
            existing_queue_record_id = _find_active_unsent_initial_request_record_id(
                email_queue_table,
                vendor_name,
            )
            if existing_queue_record_id:
                logger.info(
                    "skipped duplicate active request: %s (%s), existing_queue_record=%s",
                    vendor_name,
                    vendor_id,
                    existing_queue_record_id,
                )
                continue
        except Exception as exc:
            logger.error(
                "Airtable error checking existing queue record for vendor %s (%s): %s",
                vendor_name,
                vendor_id,
                exc,
            )
            continue

        try:
            queue_record = _create_queue_record(
                email_queue_table=email_queue_table,
                vendor_id=vendor_id,
                vendor_name=vendor_name,
                vendor_email=vendor_email,
                client_name=client_name,
                client_id=client_id,
            )
            if not queue_record:
                continue
            logger.info(
                "queue created: vendor=%s (%s), queue_record=%s",
                vendor_name,
                vendor_id,
                queue_record.get("id", "unknown"),
            )
        except Exception as exc:
            logger.error("Airtable error creating queue record for vendor %s (%s): %s", vendor_name, vendor_id, exc)
            continue

        try:
            _reset_vendor_trigger(vendors_table, vendor_id)
            logger.info("vendor trigger reset: %s (%s)", vendor_name, vendor_id)
        except Exception as exc:
            logger.error("Airtable error resetting Send Request for vendor %s (%s): %s", vendor_name, vendor_id, exc)

    logger.info("module_17_queue_initial_requests: complete")


if __name__ == "__main__":
    run()