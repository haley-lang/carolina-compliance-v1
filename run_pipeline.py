import fcntl
import logging
import os
import subprocess
import sys
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# This script (originally referred to in code comments as the
# "cron-imap-poll" cycle — see email_monitor.py's _cleanup_old_uploads
# docstring) is meant to run frequently and independently, not once a day —
# see the recommended Railway cron schedule in railway.toml. An advisory
# lock on this file prevents two overlapping invocations — of *either* the
# main() batch path or the --reprocess CLI path below, since both mutate
# Incoming Extractions / Insurance Policies / Insurance Certificates and
# neither is safe to run concurrently with the other — from processing the
# same rows at the same time. Released automatically on process exit
# (including a crash) since flock is held by the OS against the open fd, not
# tracked via a stale PID file.
PIPELINE_LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".pipeline.lock")


@contextmanager
def pipeline_lock():
    """Acquire PIPELINE_LOCK_PATH for the duration of the `with` block.

    Yields True if the lock was acquired, False if another invocation of
    this script (main() or --reprocess) already holds it — the caller is
    responsible for checking the yielded value and bailing out without
    doing any work when it's False. Used by both entry points below so
    neither can ever run while the other is mid-flight.
    """
    lock_fd = open(PIPELINE_LOCK_PATH, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_fd.close()
        yield False
        return

    try:
        yield True
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


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

    # Acquire the overlap-prevention lock before touching anything. If
    # another invocation (this same batch path, or a --reprocess run) already
    # holds it, exit cleanly rather than running a second pass over the same
    # Incoming Documents / Incoming Extractions rows concurrently.
    with pipeline_lock() as acquired:
        if not acquired:
            logger.warning(
                "Another run_pipeline.py invocation is already in progress "
                "(lock held on %s) — exiting without running.", PIPELINE_LOCK_PATH,
            )
            return

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

        # Same lock as main() — a manual reprocess must not run concurrently
        # with a scheduled batch pass (or another reprocess) touching the
        # same Incoming Extractions / Insurance Policies / Insurance
        # Certificates rows.
        with pipeline_lock() as acquired:
            if not acquired:
                logger.warning(
                    "Another run_pipeline.py invocation is already in progress "
                    "(lock held on %s) — refusing to reprocess concurrently. "
                    "Try again once it finishes.", PIPELINE_LOCK_PATH,
                )
                sys.exit(1)

            logger.info("Manual reprocess triggered for: %s", record_id)
            from pyairtable import Api
            import config
            api = Api(config.AIRTABLE_API_KEY)
            from triage_reprocessor import reprocess_from_triage
            success = reprocess_from_triage(record_id, api)

        sys.exit(0 if success else 1)
    else:
        main()
