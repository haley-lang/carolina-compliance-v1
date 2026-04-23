import os
import logging
from pyairtable import Api
from dotenv import load_dotenv
from typing import List, Dict, Optional, Any
from airtable_constants import (
    TBL_INSURANCE_POLICIES, TBL_VENDORS,
    P_VENDOR_LINK, P_EXPIRATION_STATUS, P_LAST_REMINDER_THRESHOLD,
    V_VENDOR_NAME, V_VENDOR_EMAIL,
)

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Reminder triggers are expiration-based only.
REMINDER_EXPIRATION_STATUSES = {
    "Expired",
    "Expiring in 7 Days",
    "Expiring in 30 Days",
    "Expiring in 60 Days",
    "Expiring in 90 Days",
}

# Maps Expiration Status → the threshold value module_8 writes to Last Reminder Threshold.
# Used to avoid sending duplicate reminders at the same threshold level.
_STATUS_TO_THRESHOLD = {
    "Expiring in 90 Days": "90",
    "Expiring in 60 Days": "60",
    "Expiring in 30 Days": "30",
    "Expiring in 7 Days": "7",
    "Expired": "0",
}

# Helper functions
def get_env_var(var_name: str) -> str:
    """Get an environment variable or raise an error if not found."""
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable {var_name} not found.")
    return value

def extract_first_linked_id(value: Optional[List[str]]) -> Optional[str]:
    """Extract the first linked record ID from a list of linked records."""
    if value and isinstance(value, list) and len(value) > 0:
        return value[0]
    return None

def extract_all_linked_ids(value: Optional[List[str]]) -> List[str]:
    """Extract all linked record IDs from a list of linked records."""
    if value and isinstance(value, list):
        return value
    return []

def connect_to_airtable(api_key: str, base_id: str, table_name: str) -> Api:
    """Connect to an Airtable table."""
    api = Api(api_key)
    return api.table(base_id, table_name)

def log_vendor_reminder(vendor_name: str, vendor_email: str, reason: str) -> None:
    """Log the vendor reminder details."""
    logging.info(f"Vendor Reminder Required\nVendor: {vendor_name}\nEmail: {vendor_email}\nReason: {reason}")


def policy_triggers_reminder(policy_fields: Dict[str, Any]) -> bool:
    """Return True when a policy should generate a reminder.

    Checks Last Reminder Threshold to avoid duplicate reminders at the same
    threshold level.  Module_8 writes the threshold value when the status
    changes, so if the current status maps to the same threshold already
    recorded, the reminder was already sent at this level — skip it.
    """
    status = policy_fields.get(P_EXPIRATION_STATUS)
    if status not in REMINDER_EXPIRATION_STATUSES:
        return False

    expected_threshold = _STATUS_TO_THRESHOLD.get(status, "")
    last_threshold = str(policy_fields.get(P_LAST_REMINDER_THRESHOLD) or "").strip()

    if expected_threshold and expected_threshold == last_threshold:
        return False

    return True

# Main function
def get_vendors_needing_reminders() -> List[Dict[str, Any]]:
    logging.info("Vendor Reminder Engine start")

    # Get environment variables
    api_key = get_env_var("AIRTABLE_API_KEY")
    base_id = get_env_var("AIRTABLE_BASE_ID")

    # Connect to Airtable tables
    policies_table = connect_to_airtable(api_key, base_id, TBL_INSURANCE_POLICIES)
    vendors_table = connect_to_airtable(api_key, base_id, TBL_VENDORS)

    # Load insurance policies
    try:
        policies = policies_table.all()
        logging.info(f"Policies loaded: {len(policies)}")
    except Exception as e:
        logging.error(f"Failed to load insurance policies: {e}")
        return []

    # Load vendors
    try:
        vendors = vendors_table.all()
        logging.info(f"Vendors loaded: {len(vendors)}")
    except Exception as e:
        logging.error(f"Failed to load vendors: {e}")
        return []

    # Build vendor lookup dictionary
    vendor_lookup = {vendor['id']: vendor for vendor in vendors}

    # Initialize list to track vendors needing reminders
    # Each entry is (vendor_record, [triggering_policy_records])
    vendors_needing_reminders = []
    # Map vendor_id → list of triggering policies
    vendor_policies: Dict[str, List[Dict[str, Any]]] = {}
    seen_vendor_ids = set()

    # Identify vendors needing reminders based on insurance policies
    for policy in policies:
        fields = policy['fields']
        if policy_triggers_reminder(fields):
            vendor_id = extract_first_linked_id(
                fields.get(P_VENDOR_LINK) or fields.get("Vendor")
            )
            if not vendor_id:
                logging.debug("Policy %s has no vendor link — skipping", policy.get("id"))
                continue
            vendor = vendor_lookup.get(vendor_id)
            if not vendor:
                logging.debug("Vendor %s not found in lookup — skipping", vendor_id)
                continue
            vendor_name = vendor.get("fields", {}).get(V_VENDOR_NAME, "Unknown Vendor")
            vendor_email = (
                vendor.get("fields", {}).get(V_VENDOR_EMAIL)
                or vendor.get("fields", {}).get("Email")
                or ""
            )
            if not vendor_email:
                if vendor_id not in seen_vendor_ids:
                    logging.warning("Vendor %s (%s) has no email address — skipping reminder", vendor_name, vendor_id)
                    seen_vendor_ids.add(vendor_id)
                continue

            # Collect triggering policies per vendor
            if vendor_id not in vendor_policies:
                vendor_policies[vendor_id] = []
            vendor_policies[vendor_id].append(policy)

            if vendor_id not in seen_vendor_ids:
                vendors_needing_reminders.append(vendor)
                seen_vendor_ids.add(vendor_id)
                expiration_status = fields.get(P_EXPIRATION_STATUS)
                log_vendor_reminder(vendor_name, vendor_email, expiration_status)

    # Attach triggering policies to each vendor record
    results = []
    for vendor in vendors_needing_reminders:
        triggering = vendor_policies.get(vendor['id'], [])
        results.append((vendor, triggering))

    # Log the number of reminders generated
    logging.info(f"Reminders generated: {len(results)}")

    logging.info("Vendor Reminder Engine complete")
    return results

if __name__ == "__main__":
    get_vendors_needing_reminders()