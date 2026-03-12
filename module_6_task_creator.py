import os
from datetime import datetime, date, timedelta
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

POLICIES_TABLE = "Insurance Policies"
TASKS_TABLE = "Tasks"


def run_module_6():

    api = Api(AIRTABLE_API_KEY)
    base = api.base(AIRTABLE_BASE_ID)

    policies = base.table(POLICIES_TABLE)
    tasks = base.table(TASKS_TABLE)

    policy_records = policies.all()

    today = date.today()
    soon = today + timedelta(days=30)

    for record in policy_records:

        fields = record["fields"]
        policy_id = record["id"]

        expiration = fields.get("Expiration Date")
        vendor = fields.get("Vendor Link")

        if not expiration or not vendor:
            continue

        expiration_date = datetime.strptime(expiration, "%Y-%m-%d").date()

        if expiration_date < today:

            task_name = "Expired policy follow-up"
            priority = "Urgent"

        elif expiration_date <= soon:

            task_name = "Expiring policy renewal reminder"
            priority = "High"

        else:
            continue

        existing_tasks = tasks.all()

        duplicate = False

        for t in existing_tasks:

            t_fields = t["fields"]

            if (
                t_fields.get("Task Name") == task_name
                and t_fields.get("Policy Link") == [policy_id]
                and t_fields.get("Status") != "Completed"
            ):
                duplicate = True
                break

        if duplicate:
            continue

        due_date = today if expiration_date < today else expiration_date

        tasks.create(
            {
                "Task Name": task_name,
                "Status": "Open",
                "Priority": priority,
                "Vendor Link": vendor,
                "Policy Link": [policy_id],
                "Due Date": due_date.strftime("%Y-%m-%d"),
            }
        )

        print("Created task for policy:", policy_id)


if __name__ == "__main__":
    run_module_6()