from pyairtable import Api
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo

import config


INSURANCE_TABLE_NAME = "Insurance Certificates"
EMAIL_QUEUE_TABLE_NAME = "Email Queue"
VENDORS_TABLE_NAME = "Vendors"
SECTION_SEPARATOR = "-" * 30


def _format_et_timestamp(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return "<missing Compliance Evaluated At>"

    raw_value = value.strip()
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
        return parsed.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %I:%M %p ET")
    except Exception:
        return raw_value


def _safe_fields(record: dict) -> dict:
    if not isinstance(record, dict):
        return {}
    fields = record.get("fields")
    return fields if isinstance(fields, dict) else {}


def _contains_required_policy_expired(value: object) -> bool:
    token = "REQUIRED_POLICY_EXPIRED"
    if isinstance(value, str):
        return token in value
    if isinstance(value, list):
        return any(token in str(item) for item in value)
    if isinstance(value, dict):
        return any(token in str(item) for item in value.values())
    return False


def _connect_api() -> Optional[Api]:
    api_key = (config.AIRTABLE_API_KEY or "").strip()
    base_id = (config.AIRTABLE_BASE_ID or "").strip()

    if not api_key or not base_id:
        print("ERROR: Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID in environment.")
        return None

    try:
        return Api(api_key)
    except Exception as exc:
        print(f"ERROR: Could not initialize Airtable API client: {exc}")
        return None


def _load_table_records(api: Api, table_name: str) -> Optional[list[dict]]:
    try:
        table = api.table(config.AIRTABLE_BASE_ID, table_name)
        return table.all()
    except Exception as exc:
        print(f"ERROR: Failed to query table '{table_name}': {exc}")
        return None


def _print_compliance_overview(api: Api) -> None:
    print(f"\n{SECTION_SEPARATOR}")
    print("1) Compliance Overview")
    print(SECTION_SEPARATOR)

    insurance_records = _load_table_records(api, INSURANCE_TABLE_NAME)
    if insurance_records is None:
        return

    vendor_records = _load_table_records(api, VENDORS_TABLE_NAME)
    if vendor_records is None:
        return

    fully_compliant = 0
    expired_policies = 0
    edits_requested = 0

    for record in insurance_records:
        fields = _safe_fields(record)
        compliance_status = fields.get("Compliance Status")
        deficiency_queue_status = fields.get("Deficiency Email Queue Status")
        failure_reasons = fields.get("Compliance Failure Reasons")

        if compliance_status == "Compliant":
            fully_compliant += 1

        if compliance_status == "Non-Compliant" and deficiency_queue_status == "Queued":
            edits_requested += 1

        if _contains_required_policy_expired(failure_reasons):
            expired_policies += 1

    missing_certificates = 0
    for record in vendor_records:
        links = _safe_fields(record).get("Insurance Certificates (New)")
        if not links:
            missing_certificates += 1

    print("COMPLIANCE OVERVIEW")
    print(f"- FULLY COMPLIANT: {fully_compliant}")
    print(f"- EXPIRED POLICIES: {expired_policies}")
    print(f"- EDITS REQUESTED: {edits_requested}")
    print(f"- MISSING CERTIFICATES: {missing_certificates}")


def _print_email_queue_metrics(api: Api) -> Optional[list[dict]]:
    print(f"\n{SECTION_SEPARATOR}")
    print("2) Email Queue")
    print(SECTION_SEPARATOR)
    records = _load_table_records(api, EMAIL_QUEUE_TABLE_NAME)
    if records is None:
        return None

    pending_active = 0
    sent = 0

    for record in records:
        fields = _safe_fields(record)
        email_status = fields.get("Email Status")
        record_status = fields.get("Record Status")

        if email_status == "Pending" and record_status == "Active":
            pending_active += 1
        if email_status == "Sent":
            sent += 1

    print(f"- Pending + Active: {pending_active}")
    print(f"- Sent: {sent}")
    if pending_active > 0:
        print("⚠️ ACTION NEEDED: Emails waiting to be sent")

    return records


def _print_queue_breakdown(records: Optional[list[dict]]) -> None:
    print(f"\n{SECTION_SEPARATOR}")
    print("3) Queue Breakdown")
    print(SECTION_SEPARATOR)
    if records is None:
        return

    pending_initial_request = 0
    pending_deficiency_request = 0
    sent_initial_request = 0
    sent_deficiency_request = 0

    for record in records:
        fields = _safe_fields(record)
        email_type = fields.get("Email Type")
        email_status = fields.get("Email Status")

        if email_type == "Initial Request" and email_status == "Pending":
            pending_initial_request += 1
        elif email_type == "Deficiency Request" and email_status == "Pending":
            pending_deficiency_request += 1
        elif email_type == "Initial Request" and email_status == "Sent":
            sent_initial_request += 1
        elif email_type == "Deficiency Request" and email_status == "Sent":
            sent_deficiency_request += 1

    print(f"- Pending Initial Request: {pending_initial_request}")
    print(f"- Pending Deficiency Request: {pending_deficiency_request}")
    print(f"- Sent Initial Request: {sent_initial_request}")
    print(f"- Sent Deficiency Request: {sent_deficiency_request}")


def _print_recent_activity(api: Api) -> None:
    print(f"\n{SECTION_SEPARATOR}")
    print("5) Recent Activity")
    print(SECTION_SEPARATOR)
    try:
        table = api.table(config.AIRTABLE_BASE_ID, INSURANCE_TABLE_NAME)
        records = table.all(
            sort=["-Compliance Evaluated At"],
            max_records=10,
        )
    except Exception as exc:
        print(f"ERROR: Failed to query table '{INSURANCE_TABLE_NAME}' for recent activity: {exc}")
        return

    if not records:
        print("- No recent Insurance Certificates records found.")
        return

    for record in records:
        fields = _safe_fields(record)
        record_id = record.get("id", "<missing id>")
        status = fields.get("Compliance Status", "<missing Compliance Status>")
        evaluated_at = _format_et_timestamp(fields.get("Compliance Evaluated At"))
        print(f"- {record_id} | {status} | {evaluated_at}")


def main() -> None:
    print("Carolina Compliance Solutions - Dashboard Metrics")
    print("=" * 50)

    api = _connect_api()
    if api is None:
        return

    _print_compliance_overview(api)
    email_queue_records = _print_email_queue_metrics(api)
    _print_queue_breakdown(email_queue_records)
    _print_recent_activity(api)


if __name__ == "__main__":
    main()