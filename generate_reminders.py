"""Generate generic reminder queue records for expiring/expired policies.

This script reads Insurance Policies from Airtable and adds reminder rows to
the Email Queue table for policies with Status of:
  - Expiring Soon
  - Expired

It does not send email. It only queues reminders.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from pyairtable import Api


load_dotenv()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


POLICY_TABLE_NAME = "Insurance Policies"
QUEUE_TABLE_NAME = "Email Queue"
VENDOR_LINK_FIELD_NAME = "Vendor Link"

TARGET_STATUSES = {"Expiring Soon", "Expired", "Needs Review"}
EMAIL_TYPE_BY_STATUS = {
    "Expiring Soon": "Expiration Reminder",
    "Expired": "Expired Coverage Reminder",
    "Needs Review": "Updated COI Needed",
}


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def _first_linked_id(value: Any) -> Optional[str]:
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, str):
            return first
    return None


def _build_subject(status: str) -> str:
    if status == "Expiring Soon":
        return "Updated insurance documentation requested"
    if status == "Needs Review":
        return "Updated COI documentation requested"
    return "Updated insurance documentation needed"


def _build_body(status: str) -> str:
    if status == "Expiring Soon":
        status_line = "is expiring soon"
    elif status == "Expired":
        status_line = "has expired"
    else:
        status_line = "needs review"
    return (
        "Hello,\n\n"
        f"Our records indicate a policy {status_line}. "
        "Please provide updated insurance documentation when available. "
        "Please reply to this email with your updated certificate of insurance attached, "
        "or send it to coi@carolinacompliancesolutions.com.\n\n"
        "Thank you,\n"
        "Carolina Compliance Solutions"
    )


def _existing_duplicate_keys(queue_records: list[Dict[str, Any]]) -> set[tuple[str, str]]:
    duplicate_keys: set[tuple[str, str]] = set()

    for record in queue_records:
        fields: Dict[str, Any] = record.get("fields", {})
        vendor_id = _first_linked_id(fields.get("Vendor"))
        policy_id = _first_linked_id(fields.get("Policy"))
        email_type = fields.get("Email Type")
        reminder_status = fields.get("Reminder Status")

        logger.info(
            "DEBUG existing queue record: vendor_id=%s policy_id=%s email_type=%r reminder_status=%r",
            vendor_id,
            policy_id,
            email_type,
            reminder_status,
        )

        if not policy_id or not email_type:
            continue

        if reminder_status in (None, "", "Queued", "Unsent"):
            duplicate_keys.add((policy_id, email_type))

    return duplicate_keys


def _table_field_names(table: Any) -> set[str]:
    """Return Airtable field names for a table, or an empty set if unavailable."""
    try:
        schema = table.schema()
        return {field.name for field in schema.fields}
    except Exception as exc:
        logger.warning("Could not load schema for table '%s': %s", table.name, exc)
        return set()


def _filter_existing_fields(values: Dict[str, Any], allowed_fields: set[str]) -> Dict[str, Any]:
    if not allowed_fields:
        return values
    return {k: v for k, v in values.items() if k in allowed_fields}


def run() -> None:
    api_key = _require_env("AIRTABLE_API_KEY")
    base_id = _require_env("AIRTABLE_BASE_ID")

    api = Api(api_key)
    policies_table = api.table(base_id, POLICY_TABLE_NAME)
    queue_table = api.table(base_id, QUEUE_TABLE_NAME)
    queue_field_names = _table_field_names(queue_table)
    existing_queue_records = queue_table.all()
    existing_duplicate_keys = _existing_duplicate_keys(existing_queue_records)

    logger.info("Reading policy status from field: Status")
    logger.info("Reading vendor link from field: %s", VENDOR_LINK_FIELD_NAME)
    logger.info(
        "Loaded existing queue records: total=%s duplicate_keys=%s",
        len(existing_queue_records),
        len(existing_duplicate_keys),
    )

    policies = policies_table.all()

    scanned = 0
    queued = 0
    duplicates_skipped = 0

    for policy in policies:
        scanned += 1
        fields: Dict[str, Any] = policy.get("fields", {})
        policy_id = policy.get("id")
        status = fields.get("Status")
        vendor_id = _first_linked_id(fields.get(VENDOR_LINK_FIELD_NAME))
        has_vendor_link = bool(vendor_id)
        qualifies_for_queueing = status in TARGET_STATUSES and has_vendor_link

        logger.info(
            "Scanned policy %s: status=%r vendor_link_exists=%s qualifies_for_queueing=%s",
            policy_id,
            status,
            has_vendor_link,
            qualifies_for_queueing,
        )

        if status not in TARGET_STATUSES:
            logger.info(
                "Skipping policy %s: status %r not in target statuses",
                policy_id,
                status,
            )
            continue

        if not vendor_id:
            logger.warning("Skipping policy %s: missing Vendor link", policy_id)
            continue

        email_type = EMAIL_TYPE_BY_STATUS[status]
        duplicate_key = (policy["id"], email_type)
        logger.info(
            "DEBUG duplicate check: vendor_id=%s policy_id=%s email_type=%s duplicate_key=%s key_exists=%s",
            vendor_id,
            policy["id"],
            email_type,
            duplicate_key,
            duplicate_key in existing_duplicate_keys,
        )

        if duplicate_key in existing_duplicate_keys:
            duplicates_skipped += 1
            continue

        queue_fields = {
            "Vendor": [vendor_id],
            "Policy": [policy["id"]],
            "Email Type": email_type,
            "Reminder Status": "Queued",
            "Record Status": "Active",
            "Subject": _build_subject(status),
            "Body": _build_body(status),
            "Reminder Created At": datetime.now(timezone.utc).isoformat(),
            "Reminder Reason": status,
        }
        queue_fields = _filter_existing_fields(queue_fields, queue_field_names)

        if not queue_fields:
            logger.warning("Skipping queue create for policy %s: no matching Email Queue fields", policy["id"])
            continue

        queue_table.create(queue_fields)
        logger.info("DEBUG queue create payload: %s", queue_fields)
        existing_duplicate_keys.add(duplicate_key)
        queued += 1

    logger.info("Policies scanned: %s", scanned)
    logger.info("Reminders queued: %s", queued)
    logger.info("Duplicates skipped: %s", duplicates_skipped)


if __name__ == "__main__":
    run()