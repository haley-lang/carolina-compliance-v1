# module_15_email_queue_builder.py

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
from pyairtable import Table
from module_12_vendor_reminder_engine import get_vendors_needing_reminders
from module_17_recipient_resolver import resolve_recipients
from airtable_client import Api

# Initialize logging
logging.basicConfig(level=logging.INFO)

def load_env_variables():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    return api_key, base_id

def connect_to_airtable():
    # Connect to Airtable
    api_key, base_id = load_env_variables()
    return Table(api_key, base_id, "Email Queue")

def compute_next_send_window():
    # Compute the next valid send window
    now = datetime.now()
    # Define the preferred send window
    preferred_days = [1, 2, 3]  # Tuesday, Wednesday, Thursday
    preferred_start_hour = 9
    preferred_end_hour = 11

    # Check if current time is within the preferred window
    if now.weekday() in preferred_days and preferred_start_hour <= now.hour < preferred_end_hour:
        return now

    # Calculate the next valid send window
    days_ahead = (preferred_days[0] - now.weekday() + 7) % 7
    if days_ahead == 0 and now.hour >= preferred_end_hour:
        days_ahead = 7  # Move to the next week

    next_send_date = now + timedelta(days=days_ahead)
    next_send_time = next_send_date.replace(hour=preferred_start_hour, minute=0, second=0, microsecond=0)
    return next_send_time

def collect_reminder_reasons(vendor_id: str) -> List[str]:
    # Collect reminder reasons for a vendor
    # Placeholder logic to collect reminder reasons
    # This should interact with module_12_vendor_reminder_engine
    return ["Expired Policy", "Needs Review"]

def generate_email_subject(reasons: List[str]) -> str:
    # Generate email subject based on reasons
    if "Expired Policy" in reasons:
        return f"Insurance Update Required – {reasons[0]}"
    return "Action Required – Compliance Update Needed"

def generate_email_body(vendor_name: str, reasons: List[str]) -> str:
    # Generate email body
    reasons_list = "\n".join(reasons)
    return (
        f"Dear {vendor_name},\n\n"
        f"We have identified the following reasons requiring your attention:\n"
        f"{reasons_list}\n\n"
        "Please provide an updated Certificate of Insurance (COI) at your earliest convenience.\n\n"
        "Thank you,\n"
        "Compliance Team"
    )

def create_email_queue_record(vendor: Dict[str, Any], subject: str, body: str):
    # Create a record in the Email Queue table
    table = connect_to_airtable()
    record = {
        "Vendor": vendor["name"],
        "Primary Email": vendor["primary_email"],
        "CC Emails": vendor.get("cc_emails", []),
        "Subject": subject,
        "Body": body,
        "Reminder Reasons": vendor["reasons"],
        "Reminder Status": "Queued",
        "Send After": vendor["send_after"].isoformat(),
        "Follow-Up Count": 0,
        "Created At": datetime.now().isoformat()
    }
    table.create(record)
    logging.info(f"Queue record created for vendor: {vendor['name']}")

def detect_duplicates(vendor_id: str, send_after: datetime) -> bool:
    # Detect duplicate email queue records
    table = connect_to_airtable()
    records = table.all(formula=f"AND(Vendor = '{vendor_id}', Reminder Status = 'Queued', Send After = '{send_after.isoformat()}')")
    return len(records) > 0

def run():
    logging.info("Module start")
    # Main logic to build the email queue
    try:
        vendors = get_vendors_needing_reminders()
    except Exception as e:
        logging.error(f"Error loading vendors: {e}")
        return
    logging.info(f"Vendors loaded: {len(vendors)}")

    for vendor in vendors:
        try:
            recipients = resolve_recipients(vendor["id"])
        except Exception as e:
            logging.error(f"Error resolving recipients for vendor {vendor['name']}: {e}")
            continue
        vendor["primary_email"] = recipients["primary_email"]
        vendor["cc_emails"] = recipients.get("cc_emails", [])

        try:
            vendor["reasons"] = collect_reminder_reasons(vendor["id"])
        except Exception as e:
            logging.error(f"Error collecting reminder reasons for vendor {vendor['name']}: {e}")
            continue
        subject = generate_email_subject(vendor["reasons"])
        body = generate_email_body(vendor["name"], vendor["reasons"])

        send_after = compute_next_send_window()
        vendor["send_after"] = send_after

        try:
            if not detect_duplicates(vendor["id"], send_after):
                create_email_queue_record(vendor, subject, body)
            else:
                logging.info(f"Duplicate skipped for vendor: {vendor['name']}")
        except Exception as e:
            logging.error(f"Error processing vendor {vendor['name']}: {e}")
        else:
            logging.info(f"Duplicate skipped for vendor: {vendor['name']}")

    logging.info("Module complete")

if __name__ == "__main__":
    run()