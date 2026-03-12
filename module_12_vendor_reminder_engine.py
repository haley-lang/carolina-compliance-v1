import os
import logging
from pyairtable import Api
from dotenv import load_dotenv
from typing import List, Dict, Optional

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# Main function
def get_vendors_needing_reminders() -> List[Dict[str, Any]]:
    logging.info("Vendor Reminder Engine start")

    # Get environment variables
    api_key = get_env_var("AIRTABLE_API_KEY")
    base_id = get_env_var("AIRTABLE_BASE_ID")

    # Connect to Airtable tables
    policies_table = connect_to_airtable(api_key, base_id, "Insurance Policies")
    vendor_assignments_table = connect_to_airtable(api_key, base_id, "Vendor Client Assignments")
    vendors_table = connect_to_airtable(api_key, base_id, "Vendors")

    # Load insurance policies
    try:
        policies = policies_table.all()
        logging.info(f"Policies loaded: {len(policies)}")
    except Exception as e:
        logging.error(f"Failed to load insurance policies: {e}")
        return []

    # Load vendor client assignments
    try:
        vendor_assignments = vendor_assignments_table.all()
        logging.info(f"Assignments loaded: {len(vendor_assignments)}")
    except Exception as e:
        logging.error(f"Failed to load vendor client assignments: {e}")
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
    vendors_needing_reminders = []

    # Identify vendors needing reminders based on insurance policies
    for policy in policies:
        fields = policy['fields']
        expiration_status = fields.get("Expiration Status")
        if expiration_status in ["Expired", "Expiring in 7 Days", "Expiring in 30 Days", "Expiring in 90 Days"]:
            vendor_id = extract_first_linked_id(fields.get("Vendor"))
            vendor = vendor_lookup.get(vendor_id)
            if vendor and vendor_id not in vendors_needing_reminders:
                vendors_needing_reminders.append(vendor)

    # Identify vendors needing reminders based on compliance status
    for assignment in vendor_assignments:
        fields = assignment['fields']
        compliance_status = fields.get("Compliance Status")
        if compliance_status in ["Missing Coverage", "Expired", "Needs Review"]:
            vendor_id = extract_first_linked_id(fields.get("Vendor"))
            vendor = vendor_lookup.get(vendor_id)
            if vendor and vendor_id not in vendors_needing_reminders:
                vendors_needing_reminders.append(vendor)

    # Log the number of reminders generated
    logging.info(f"Reminders generated: {len(vendors_needing_reminders)}")

    logging.info("Vendor Reminder Engine complete")
    return vendors_needing_reminders

if __name__ == "__main__":
    run()