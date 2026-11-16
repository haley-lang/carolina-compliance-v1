"""
CLI entrypoint: refresh the Site Knowledge base from the CCS website.
Run directly or from a Railway weekly cron.

Usage:
    .venv/bin/python site_knowledge_refresh.py
"""
import logging
import os
import sys

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main() -> int:
    if not os.environ.get("AIRTABLE_API_KEY"):
        log.error("AIRTABLE_API_KEY not set")
        return 1

    from site_knowledge import refresh_all
    log.info("Starting site knowledge refresh...")
    summary = refresh_all()
    log.info(
        "Done — pages_found=%d  updated=%d  errors=%d",
        summary["pages_found"], summary["updated"], summary["errors"],
    )
    return 1 if summary["errors"] and summary["updated"] == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
