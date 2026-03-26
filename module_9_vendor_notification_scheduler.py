import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from pyairtable import Api

# Load environment variables from .env file
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Read Airtable configuration from environment variables
API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
VENDORS_TABLE = 'Vendors'
INSURANCE_POLICIES_TABLE = 'Insurance Policies'

def get_expiring_policies(api):
    table = api.table(BASE_ID, INSURANCE_POLICIES_TABLE)
    formula = "OR({Expiration Status} = 'Expiring in 30 Days', {Expiration Status} = 'Expired')"
    return table.all(formula=formula)

def log_vendor_notifications(api):
    # Log expiring policies
    expiring_policies = get_expiring_policies(api)
    for policy in expiring_policies:
        vendor_id = policy['fields'].get('Vendor', [None])[0] or policy['fields'].get('Vendor Link', [None])[0]
        if not vendor_id:
            client_vendors_id = policy['fields'].get('Client Vendors', [None])[0]
            if client_vendors_id:
                client_vendors_table = api.table(BASE_ID, 'Client Vendors')
                try:
                    client_vendor_record = client_vendors_table.get(client_vendors_id)
                    vendor_id = client_vendor_record['fields'].get('Vendor', [None])[0]
                except Exception as e:
                    logging.error(f"Error fetching client vendor: {e}")
                    vendor_id = None

        vendor_name = get_vendor_name(api, vendor_id, "Insurance Policies")
        expiration_status = policy['fields'].get('Expiration Status')
        logging.info(f"Vendor needs expiration notice: {vendor_name} — {expiration_status}")

    logging.info("Compliance/requirement-based reminder triggers are disabled.")

def get_vendor_name(api, vendor_id, source):
    if isinstance(vendor_id, list):
        vendor_id = vendor_id[0] if vendor_id else None
    if not vendor_id:
        logging.info(f"Vendor link is missing or empty for record from {source}.")
        return "Unknown Vendor"
    table = api.table(BASE_ID, VENDORS_TABLE)
    try:
        vendor = table.get(vendor_id)
        vendor_name = vendor['fields'].get('Vendor Name', 'Unknown Vendor')
        logging.debug(f"Record ID: {vendor_id}, Source: {source}, Vendor Link: {vendor_id}")
        return vendor_name
    except Exception as e:
        logging.error(f"Error fetching vendor: {e}")
        return "Unknown Vendor"

def run():
    if not API_KEY or not BASE_ID:
        logging.error("Airtable API key or Base ID is missing.")
        return

    api = Api(API_KEY)
    log_vendor_notifications(api)

if __name__ == "__main__":
    run()