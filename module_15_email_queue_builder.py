# module_15_email_queue_builder.py

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
from pyairtable import Table, Api
from module_12_vendor_reminder_engine import get_vendors_needing_reminders

# Initialize logging
logging.basicConfig(level=logging.INFO)

HARDCODED_EMAIL_SUBJECT = "Insurance Update Required"
HARDCODED_EMAIL_BODY = (
    "Dear Vendor,\n\n"
    "Please provide an updated Certificate of Insurance (COI) at your earliest convenience.\n\n"
    "Please reply to this email with your updated certificate of insurance attached, or send it to coi@carolinacompliancesolutions.com.\n\n"
    "Thank you,\n"
    "Compliance Team"
)

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


def connect_api() -> Api:
    api_key, _ = load_env_variables()
    return Api(api_key)

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

def create_email_queue_record(vendor: Dict[str, Any], subject: str, body: str):
    # Create a record in the Email Queue table
    table = connect_to_airtable()
    vendor_name = vendor.get("fields", {}).get("Vendor Name") or vendor.get("fields", {}).get("Name") or "Unknown Vendor"
    primary_email = vendor.get("fields", {}).get("Email", "")
    record = {
        "Vendor": vendor_name,
        "Primary Email": primary_email,
        "CC Emails": [],
        "Subject": subject,
        "Body": body,
        "Reminder Reasons": [],
        "Reminder Status": "Queued",
        "Record Status": "Active",
        "Send After": vendor["send_after"].isoformat(),
        "Follow-Up Count": 0,
        "Created At": datetime.now().isoformat()
    }
    table.create(record)
    logging.info(f"Queue record created for vendor: {vendor_name}")

def detect_duplicates(vendor_name: str, send_after: datetime) -> bool:
    # Detect duplicate email queue records
    table = connect_to_airtable()
    records = table.all(formula=f"AND(Vendor = '{vendor_name}', Reminder Status = 'Queued', Send After = '{send_after.isoformat()}')")
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
        vendor_name = vendor.get("fields", {}).get("Vendor Name") or vendor.get("fields", {}).get("Name") or "Unknown Vendor"
        primary_email = vendor.get("fields", {}).get("Email", "")
        if not primary_email:
            logging.warning(f"Skipping vendor {vendor_name}: missing primary email")
            continue

        subject = HARDCODED_EMAIL_SUBJECT
        body = HARDCODED_EMAIL_BODY

        send_after = compute_next_send_window()
        vendor["send_after"] = send_after

        try:
            if not detect_duplicates(vendor_name, send_after):
                create_email_queue_record(vendor, subject, body)
            else:
                logging.info(f"Duplicate skipped for vendor: {vendor_name}")
        except Exception as e:
            logging.error(f"Error processing vendor {vendor_name}: {e}")

    logging.info("Module complete")

if __name__ == "__main__":
    run()