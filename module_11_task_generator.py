import os
import logging
from datetime import datetime
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

def extract_first_linked_record_id(linked_records: List[Dict]) -> Optional[str]:
    """Extract the first linked record ID from a list of linked records."""
    if linked_records:
        return linked_records[0].get('id')
    return None

def extract_all_linked_record_ids(linked_records: List[Dict]) -> List[str]:
    """Extract all linked record IDs from a list of linked records."""
    return [record.get('id') for record in linked_records if 'id' in record]

def build_task_values(expiration_status: str, expiration_date: str) -> Optional[Dict]:
    """Build task values based on expiration status."""
    today = datetime.today().strftime('%Y-%m-%d')
    if expiration_status == "Expired":
        return {
            "Task Name": "Expired policy follow-up",
            "Status": "Open",
            "Priority": "Urgent",
            "Due Date": today
        }
    elif expiration_status in ["Expiring in 7 Days", "Expiring in 30 Days", "Expiring in 90 Days"]:
        return {
            "Task Name": "Renew insurance policy",
            "Status": "Open",
            "Priority": "High",
            "Due Date": expiration_date
        }
    return None

def load_existing_open_task_keys(tasks_table) -> List[Dict]:
    """Load existing open task keys to prevent duplicates."""
    try:
        open_tasks = tasks_table.all(formula="Status='Open'")
    except Exception as e:
        logging.error(f"Failed to load open tasks: {e}")
        return []
    return [{"Task Name": task['fields'].get("Task Name"), "Policy Link": task['fields'].get("Policy Link")} for task in open_tasks]

def connect_to_airtable(api_key: str, base_id: str, table_name: str):
    """Connect to an Airtable table."""
    api = Api(api_key)
    return api.table(base_id, table_name)

# Main function
def run():
    logging.info("Module start")

    # Get environment variables
    api_key = get_env_var("AIRTABLE_API_KEY")
    base_id = get_env_var("AIRTABLE_BASE_ID")

    # Connect to Airtable tables
    policies_table = connect_to_airtable(api_key, base_id, "Insurance Policies")
    tasks_table = connect_to_airtable(api_key, base_id, "Tasks")

    # Load records
    try:
        policies = policies_table.all()
    except Exception as e:
        logging.error(f"Failed to load policies: {e}")
        return
    logging.info(f"Number of policies loaded: {len(policies)}")

    existing_open_tasks = load_existing_open_task_keys(tasks_table)
    logging.info(f"Number of tasks loaded: {len(existing_open_tasks)}")

    tasks_created = 0
    duplicates_skipped = 0

    for policy in policies:
        fields = policy['fields']
        expiration_status = fields.get("Expiration Status")
        expiration_date = fields.get("Expiration Date")
        task_values = build_task_values(expiration_status, expiration_date)

        if task_values:
            policy_link = policy['id']
            vendor_link = fields.get("Vendor") or fields.get("Vendor Link")
            client_vendor = extract_first_linked_record_id(fields.get("Client Vendors", []))

            # Check for duplicate tasks
            if any(task for task in existing_open_tasks if task["Task Name"] == task_values["Task Name"] and task["Policy Link"] == policy_link and task["Vendor Link"] == vendor_link):
                duplicates_skipped += 1
                logging.info(f"Duplicate task skipped for policy: {policy_link}")
                continue

            # Add additional fields
            task_values.update({
                "Vendor Link": vendor_link,
                "Policy Link": policy_link,
                "Client Vendor": client_vendor
            })

            # Create task
            try:
                tasks_table.create(task_values)
                tasks_created += 1
                logging.info(f"Task created for policy: {policy_link}")
            except Exception as e:
                logging.error(f"Failed to create task for policy: {policy_link} - {e}")

    logging.info(f"Tasks created: {tasks_created}")
    logging.info(f"Duplicate tasks skipped: {duplicates_skipped}")
    logging.info("Module complete")

if __name__ == "__main__":
    run()