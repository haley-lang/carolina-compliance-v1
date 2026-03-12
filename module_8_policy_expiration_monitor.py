import os
import logging
from pyairtable import Api
from datetime import date, datetime
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Airtable configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Table name
TABLE_INSURANCE_POLICIES = "Insurance Policies"

def fetch_records(table):
    """Fetch all records from a given table."""
    try:
        records = table.all()
        logger.info("Loaded %d records from table", len(records))
        return records
    except Exception as e:
        logger.error("Failed to fetch records: %s", e)
        return []

def process_expiration_dates(records, table):
    """Process expiration dates, log their status, and update Airtable."""
    today = date.today()
    for record in records:
        policy_id = record["id"]
        expiration_date_str = record["fields"].get("Expiration Date")
        expiration_status = "Active"  # Default status

        if not expiration_date_str:
            expiration_status = "Missing Expiration Date"
            logger.warning("Policy %s has no expiration date", policy_id)
        else:
            try:
                expiration_date = datetime.fromisoformat(expiration_date_str).date()
                days_until_expiration = (expiration_date - today).days

                if days_until_expiration < 0:
                    expiration_status = "Expired"
                    logger.info("Policy already expired: %s", policy_id)
                elif days_until_expiration <= 7:
                    expiration_status = "Expiring in 7 Days"
                    logger.info("Policy expiring in 7 days: %s", policy_id)
                elif days_until_expiration <= 30:
                    expiration_status = "Expiring in 30 Days"
                    logger.info("Policy expiring in 30 days: %s", policy_id)
                elif days_until_expiration <= 90:
                    expiration_status = "Expiring in 90 Days"
                    logger.info("Policy expiring in 90 days: %s", policy_id)
            except ValueError:
                expiration_status = "Missing Expiration Date"
                logger.error("Invalid date format for policy %s: %s", policy_id, expiration_date_str)

        # Update the "Expiration Status" field in Airtable
        try:
            table.update(policy_id, {"Expiration Status": expiration_status})
            logger.info("Updated policy %s with status: %s", policy_id, expiration_status)
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