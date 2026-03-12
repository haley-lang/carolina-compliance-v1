import logging
from typing import List, Dict, Any
from pyairtable import Api
from dotenv import load_dotenv
import os

# Load environment variables
def load_environment_variables() -> None:
    load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper function to build vendor lookup
def build_vendor_lookup(vendors: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {vendor['id']: vendor['fields'] for vendor in vendors}

# Helper function to collect vendor reminder reasons
def collect_reminder_reasons(policies: List[Dict[str, Any]], assignments: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    reminder_reasons = {}
    # Collect reminder reasons from policies
    for policy in policies:
        vendor_id = policy['fields'].get('Vendor')
        expiration_status = policy['fields'].get('Expiration Status')
        if vendor_id and expiration_status:
            if vendor_id not in reminder_reasons:
                reminder_reasons[vendor_id] = []
            reminder_reasons[vendor_id].append(expiration_status)

    # Collect reminder reasons from assignments
    for assignment in assignments:
        vendor_id = assignment['fields'].get('Vendor')
        compliance_status = assignment['fields'].get('Compliance Status')
        if vendor_id and compliance_status:
            if vendor_id not in reminder_reasons:
                reminder_reasons[vendor_id] = []
            reminder_reasons[vendor_id].append(compliance_status)

    return reminder_reasons

# Helper function to choose the most important status
def choose_most_important_status(reasons: List[str]) -> str:
    priority = ['Expired', 'Expiring in 7 Days', 'Expiring in 30 Days', 'Expiring in 90 Days', 'Needs Review']
    for status in priority:
        if status in reasons:
            return status
    return 'Needs Review'

# Main function to run the module
def run() -> None:
    logging.info("Module start")
    load_environment_variables()

    # Initialize Airtable API with base ID
    base_id = os.getenv('AIRTABLE_BASE_ID')
    api = Api(os.getenv('AIRTABLE_API_KEY'))

    # Load data from Airtable
    vendors = api.table(base_id, 'Vendors').all()
    policies = api.table(base_id, 'Insurance Policies').all()
    assignments = api.table(base_id, 'Vendor Client Assignments').all()

    logging.info("Vendors loaded")
    logging.info("Policies loaded")
    logging.info("Assignments loaded")

    vendor_lookup = build_vendor_lookup(vendors)

    # Generate email previews
    for vendor_id, vendor_info in vendor_lookup.items():
        email = vendor_info.get('Email')
        if not email:
            vendor_name = vendor_info.get('Name', 'Unknown Vendor')
            logging.warning(f"Skipping vendor {vendor_name} due to missing email.")
            continue

        reasons = reminder_reasons.get(vendor_id, [])
        if not reasons:
            continue

        most_important_status = choose_most_important_status(reasons)
        if any(status in reasons for status in ['Expired', 'Expiring in 7 Days', 'Expiring in 30 Days', 'Expiring in 90 Days']):
            subject = f"Insurance Update Required – {most_important_status}"
            body = (
                f"Dear {vendor_info.get('Name')},\n\n"
                "Our records indicate that one or more of your insurance policies currently require attention.\n\n"
                "Reasons identified:\n"
                + "\n".join(f"- {reason}" for reason in reasons) +
                "\n\nTo remain compliant, please provide an updated certificate of insurance showing current coverage.\n\n"
                "If you have already submitted an update, please disregard this message.\n\n"
                "Best regards\nCompliance Team\nCarolina Compliance Solutions"
            )
        else:
            subject = "Action Required – Compliance Update Needed"
            body = (
                f"Dear {vendor_info.get('Name')},\n\n"
                "During a recent compliance review, we found that your current insurance documentation requires attention.\n\n"
                "Reasons identified:\n"
                + "\n".join(f"- {reason}" for reason in reasons) +
                "\n\nTo remain approved for work, please submit updated insurance documentation as soon as possible.\n\n"
                "If you believe this notice was sent in error, please let us know.\n\n"
                "Best regards\nCompliance Team\nCarolina Compliance Solutions"
            )

        logging.info(f"Email preview for {vendor_info.get('Name')}:\nSubject: {subject}\nBody:\n{body}\n")

    logging.info("Email previews generated")
    logging.info("Module complete")

if __name__ == "__main__":
    run()