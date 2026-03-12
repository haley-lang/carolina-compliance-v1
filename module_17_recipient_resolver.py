import os
import logging
from typing import List, Dict, Optional
from pyairtable import Api
from dotenv import load_dotenv

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

def connect_to_airtable_table(api_key: str, base_id: str, table_name: str) -> Optional[Api]:
    """Connect to an Airtable table, return None if table does not exist."""
    try:
        api = Api(api_key)
        return api.table(base_id, table_name)
    except Exception as e:
        logging.warning(f"Table {table_name} not found: {e}")
        return None

def normalize_email(email: str) -> str:
    """Normalize email by stripping whitespace and converting to lowercase."""
    return email.strip().lower()

def is_valid_email(email: str) -> bool:
    """Check if the email has a basic valid format."""
    return "@" in email and not email.startswith("no-reply") and email != "test@test.com"

def is_suppressed_email(email: str) -> bool:
    """Check if the email should be suppressed."""
    return not email or "@" not in email or email in ["", "test@test.com", "no-reply@example.com"]

def add_candidate_email(email: str, source: str, candidates: Dict[str, str], invalids: List[str]) -> None:
    """Add email to candidates if valid, otherwise to invalids."""
    email = normalize_email(email)
    if is_valid_email(email):
        candidates[email] = source
    else:
        invalids.append(email)

def choose_primary_email(candidates: Dict[str, str]) -> Optional[str]:
    """Choose the primary email based on priority."""
    priority_sources = ["Inbound Mailbox", "Vendor Email", "COI Contact", "Named Insured", "Other Contact"]
    for source in priority_sources:
        for email, email_source in candidates.items():
            if email_source == source:
                return email
    return None

def resolve_vendor_recipients(vendor: Dict, optional_tables: Dict[str, Optional[Api]]) -> Dict:
    """Resolve recipient data for a vendor."""
    vendor_id = vendor['id']
    vendor_name = vendor['fields'].get("Name", "Unknown Vendor")
    candidates = {}
    invalids = []

    # Add vendor email
    add_candidate_email(vendor['fields'].get("Email", ""), "Vendor Email", candidates, invalids)

    # Add optional emails from future-ready tables
    if optional_tables.get("Vendor Contact Emails"):
        # Example logic for future table
        pass

    # Choose primary email
    primary_to_email = choose_primary_email(candidates)
    cc_emails = [email for email in candidates if email != primary_to_email]

    # Log if no valid email found
    if not primary_to_email:
        logging.warning(f"No valid email found for vendor {vendor_name} (ID: {vendor_id})")

    return {
        "vendor_id": vendor_id,
        "vendor_name": vendor_name,
        "primary_to_email": primary_to_email,
        "cc_emails": cc_emails,
        "all_candidate_emails": list(candidates.keys()),
        "invalid_emails": invalids,
        "sources_used": candidates
    }

def run() -> None:
    logging.info("Recipient Resolver Module start")

    # Get environment variables
    api_key = get_env_var("AIRTABLE_API_KEY")
    base_id = get_env_var("AIRTABLE_BASE_ID")

    # Connect to Airtable tables
    vendors_table = connect_to_airtable_table(api_key, base_id, "Vendors")
    optional_tables = {
        "Vendor Contact Emails": connect_to_airtable_table(api_key, base_id, "Vendor Contact Emails"),
        "Inbound Mailbox Records": connect_to_airtable_table(api_key, base_id, "Inbound Mailbox Records"),
        "COI Extraction Results": connect_to_airtable_table(api_key, base_id, "COI Extraction Results")
    }

    # Load vendors
    if not vendors_table:
        logging.error("Vendors table not found. Exiting.")
        return

    vendors = vendors_table.all()
    logging.info(f"Vendors loaded: {len(vendors)}")

    # Resolve recipients for each vendor
    for vendor in vendors:
        recipient_data = resolve_vendor_recipients(vendor, optional_tables)
        logging.info(f"Recipient data for vendor {recipient_data['vendor_name']}: {recipient_data}")

    logging.info("Recipient Resolver Module complete")

if __name__ == "__main__":
    run()