import logging
import os
import subprocess
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

def run_module(module_name, command):
    """Run a module and log its execution."""
    try:
        logger.info("Running %s...", module_name)
        subprocess.run(command, check=True, shell=True)
        logger.info("%s completed successfully.", module_name)
    except subprocess.CalledProcessError as e:
        logger.error("Error running %s: %s", module_name, e)

def main():
    logger.info("=== Starting Carolina Compliance Solutions Pipeline ===")

    # V1 simplified flow (execution only)
    modules = [
        ("Module 1 Email Intake", ".venv/bin/python email_monitor.py"),
        ("Module 2 COI Extractor", ".venv/bin/python extractor.py"),
        ("Module 3 Airtable Importer", ".venv/bin/python airtable_importer.py"),
        ("Module 4 COI Processor", ".venv/bin/python processor.py"),
        ("Module 8 Policy Expiration Monitor", ".venv/bin/python module_8_policy_expiration_monitor.py"),
        ("Module 8B Cancellation/Endorsement/Reinstatement Handler", ".venv/bin/python module_8b.py"),
        ("Module 7B Requirement Validator", ".venv/bin/python module_7b_requirement_validator.py"),
        ("Module 17 Initial COI Request Queue", ".venv/bin/python module_17_queue_initial_requests.py"),
        ("Module 18 Vendor Initial Request Sender", ".venv/bin/python module_18_vendor_initial_request_sender.py"),
        ("Module 19 Requirements Follow-Up", ".venv/bin/python module_19_requirements_followup.py"),
        ("Module 15 Email Queue Builder", ".venv/bin/python module_15_email_queue_builder.py"),
        ("Module 10 Vendor Email Sender", ".venv/bin/python module_10_vendor_email_sender.py"),
    ]

    # Explicitly disabled for simplified V1 pipeline (kept in codebase, not executed):
    disabled_modules = [
        "module_7a_client_setup_wizard.py",
        "module_6_task_creator.py",
        "module_11_task_generator.py",
        "task_generator.py",
    ]
    logger.info("V1 disabled modules: %s", ", ".join(disabled_modules))

    # Run each module in sequence
    for module_name, command in modules:
        run_module(module_name, command)

    # ── Daily cron tasks (run as subprocess so they use the venv) ──────
    logger.info("=== Running daily cron tasks ===")
    venv_python = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".venv", "bin", "python")
    run_module("Daily Cron Tasks", f"{venv_python} daily_cron.py")

    logger.info("=== Pipeline execution complete ===")

if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--reprocess":
        record_id = sys.argv[2]
        logger.info("Manual reprocess triggered for: %s", record_id)
        from pyairtable import Api
        import config
        api = Api(config.AIRTABLE_API_KEY)
        from triage_reprocessor import reprocess_from_triage
        success = reprocess_from_triage(record_id, api)
        sys.exit(0 if success else 1)
    else:
        main()
