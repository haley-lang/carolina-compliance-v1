"""Daily cron tasks — run after the main pipeline modules."""

import logging

from pyairtable import Api
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("=== Daily cron tasks starting ===")

    api = Api(config.AIRTABLE_API_KEY)

    # Cancellation monitoring — escalate cases where effective date passed
    from module_8b import check_cancellation_monitoring
    extractions_t = api.table(config.AIRTABLE_BASE_ID, "tblT88Ty6d6M766oY")
    email_queue_t = api.table(config.AIRTABLE_BASE_ID, "tblCeRCf6RToTFkbL")
    clients_t = api.table(config.AIRTABLE_BASE_ID, "tbltnBIWke20IEI3K")
    vendors_t = api.table(config.AIRTABLE_BASE_ID, "tblsOphSd5DKSZEro")
    check_cancellation_monitoring(extractions_t, email_queue_t, clients_t, vendors_t)

    # Exception expiry check
    from exception_manager import check_expiring_exceptions
    overrides_t = api.table(config.AIRTABLE_BASE_ID, "tblKVIdTYwF1Sp5Iw")
    check_expiring_exceptions(overrides_t, email_queue_t, clients_t)

    # Triage auto-escalation — escalate Pending Internal Review items older than 48h
    from operational_email_handler import check_auto_escalation
    doc_t = api.table(config.AIRTABLE_BASE_ID, config.AIRTABLE_TABLE_NAME)
    check_auto_escalation(doc_t, email_queue_t)

    # Reprocess reviewed triage records
    from triage_reprocessor import check_reviewed_for_reprocess
    check_reviewed_for_reprocess(api)

    logger.info("=== Daily cron tasks complete ===")


if __name__ == "__main__":
    main()
