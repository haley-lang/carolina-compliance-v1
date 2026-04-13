"""
Module 8 — Policy Expiration Monitor
Scans all Insurance Policies, sets Expiration Status with thresholds at
90, 60, 30, and 7 days. Each threshold triggers only once per policy
(tracked via Last Reminder Threshold field). Also detects Pending Active
policies with future effective dates.
"""

import os
import logging
from pyairtable import Api
from datetime import date, datetime
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

TABLE_INSURANCE_POLICIES = "Insurance Policies"

# Field IDs
FLD_EXPIRATION_STATUS       = "fldXLb8RLrLc56bBx"
FLD_LAST_REMINDER_THRESHOLD = "fld0iLtDjPRVFT9Xy"
FLD_EFFECTIVE_DATE          = "fldvj6n6aR0btY1U1"

# Thresholds in descending order — first match wins
THRESHOLDS = [
    (7,  "Expiring in 7 Days"),
    (30, "Expiring in 30 Days"),
    (60, "Expiring in 60 Days"),
    (90, "Expiring in 90 Days"),
]


def fetch_records(table):
    try:
        records = table.all()
        logger.info("Loaded %d records from table", len(records))
        return records
    except Exception as e:
        logger.error("Failed to fetch records: %s", e)
        return []


def _parse_date(raw: str):
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def process_expiration_dates(records, table):
    """Process expiration dates with multi-threshold reminders."""
    today = date.today()

    for record in records:
        policy_id = record["id"]
        fields = record["fields"]
        expiration_str = fields.get("Expiration Date")
        effective_str = fields.get("Effective Date")
        current_status = fields.get("Expiration Status", "")
        last_threshold = fields.get("Last Reminder Threshold") or fields.get(FLD_LAST_REMINDER_THRESHOLD) or ""

        update_fields = {}

        # Check for future-dated policy (Pending Active)
        effective = _parse_date(effective_str)
        if effective and effective > today:
            if current_status != "Pending Active":
                update_fields[FLD_EXPIRATION_STATUS] = "Pending Active"
                logger.info("Policy %s has future effective date %s → Pending Active", policy_id, effective)
            # Future-dated policies don't need expiration checks yet
            if update_fields:
                try:
                    table.update(policy_id, update_fields, typecast=True)
                except Exception as e:
                    logger.error("Failed to update policy %s: %s", policy_id, e)
            continue

        if not expiration_str:
            new_status = "Missing Expiration Date"
            logger.warning("Policy %s has no expiration date", policy_id)
            if current_status != new_status:
                update_fields[FLD_EXPIRATION_STATUS] = new_status
        else:
            expiration = _parse_date(expiration_str)
            if not expiration:
                new_status = "Missing Expiration Date"
                logger.error("Invalid date format for policy %s: %s", policy_id, expiration_str)
                if current_status != new_status:
                    update_fields[FLD_EXPIRATION_STATUS] = new_status
            else:
                days_until = (expiration - today).days

                if days_until < 0:
                    new_status = "Expired"
                    if current_status != new_status:
                        update_fields[FLD_EXPIRATION_STATUS] = new_status
                        logger.info("Policy already expired: %s", policy_id)
                else:
                    # Find the tightest threshold that applies
                    new_status = "Active"
                    new_threshold = ""
                    for threshold_days, status_label in THRESHOLDS:
                        if days_until <= threshold_days:
                            new_status = status_label
                            new_threshold = str(threshold_days)
                            break

                    # Only update if status changed
                    if current_status != new_status:
                        update_fields[FLD_EXPIRATION_STATUS] = new_status

                    # Track threshold for reminder dedup
                    if new_threshold and new_threshold != last_threshold:
                        update_fields[FLD_LAST_REMINDER_THRESHOLD] = new_threshold
                        logger.info("Policy %s crossed %s-day threshold → %s",
                                    policy_id, new_threshold, new_status)

        if update_fields:
            try:
                table.update(policy_id, update_fields, typecast=True)
                logger.info("Updated policy %s: %s", policy_id, update_fields)
            except Exception as e:
                logger.error("Failed to update policy %s: %s", policy_id, e)


def run():
    logger.info("=== Module 8: Policy Expiration Monitor starting ===")

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        logger.error("Airtable API credentials are missing.")
        return

    api = Api(AIRTABLE_API_KEY)
    insurance_policies_table = api.table(AIRTABLE_BASE_ID, TABLE_INSURANCE_POLICIES)
    policies = fetch_records(insurance_policies_table)

    process_expiration_dates(policies, insurance_policies_table)

    logger.info("=== Policy expiration monitoring complete ===")

if __name__ == "__main__":
    run()
