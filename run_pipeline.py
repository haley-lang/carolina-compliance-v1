import logging
import subprocess

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

    # Define the modules and their commands
    modules = [
        ("Module 1 Email Intake", "python email_monitor.py"),
        ("Module 2 COI Extractor", "python extractor.py"),
        ("Module 3 Airtable Importer", "python airtable_importer.py"),
        ("Module 4 COI Processor", "python processor.py"),
        ("Module 5 Compliance Checker", "python compliance_checker.py"),
        ("Module 6 Renewal Task Creator", "python module_6_task_creator.py"),
        ("Module 7B Requirement Validator", "python module_7b_requirement_validator.py"),
    ]

    # Run each module in sequence
    for module_name, command in modules:
        run_module(module_name, command)

    logger.info("=== Pipeline execution complete ===")

if __name__ == "__main__":
    main()