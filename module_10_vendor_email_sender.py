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

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Cc

def send_queued_emails(api):
    sg = SendGridAPIClient(api_key=os.getenv("SENDGRID_API_KEY"))
    from_email = Email(os.getenv("SENDGRID_FROM_EMAIL"))
    table = api.table(BASE_ID, "Email Queue")
    records = table.all(formula="AND({Reminder Status} = 'Queued', {Send After} <= NOW())")
    record_count = len(records)
    logging.info(f"Found {record_count} queued email record(s).")
    if record_count == 0:
        logging.info("No queued email records found.")
        
    for record in records:
        fields = record['fields']
        missing_fields = [field for field in ['Primary Email', 'Subject', 'Body'] if field not in fields]
        if missing_fields:
            logging.error(f"Record ID {record['id']} is missing required fields: {', '.join(missing_fields)}")
            continue
        try:
            to_email = To(fields['Primary Email'])
            cc_emails = [Cc(email) for email in fields.get('CC Emails', [])]
            subject = fields['Subject']
            content = fields['Body']

            mail = Mail(from_email, to_email, subject, content)
            if cc_emails:
                mail.personalizations[0].add_cc(cc_emails)

            response = sg.send(mail)
            if response.status_code == 202:
                update_email_status(api, record['id'], "Sent")
            else:
                logging.error(f"Failed to send email to {fields['Primary Email']}: {response.body}")
                update_email_status(api, record['id'], "Failed")
        except Exception as e:
            logging.error(f"Failed to send email to {fields['Primary Email']}: {e}")
            update_email_status(api, record['id'], "Failed")

def update_email_status(api, record_id, status):
    table = api.table(BASE_ID, "Email Queue")
    table.update(record_id, {"Reminder Status": status})

def run():
    if not API_KEY or not BASE_ID:
        logging.error("Airtable API key or Base ID is missing.")
        return

    api = Api(API_KEY)
    send_queued_emails(api)

if __name__ == "__main__":
    run()