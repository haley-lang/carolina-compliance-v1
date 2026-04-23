"""Railway cron — advances Active follow-up ladders when scheduled times are due.

Scans Outbound Follow-Ups for Active records, inspects each ladder's
Day 2 Send At / Day 4 Send At / Day 6 Escalate At, and calls
module_22.advance_ladder for any that are due.

Schedule: daily at 08:00 ET. Railway cron runs in UTC — see railway.toml
for the translation. DST IS NOT AUTO-HANDLED by Railway cron; the cron
entry includes a comment flagging this.
"""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

import config
from module_21_business_hours import ET
from module_22_followup_ladder import (
    LADDER_TABLE_ID,
    FLD_STATUS,
    FLD_STAGE,
    FLD_DAY_2_SEND_AT,
    FLD_DAY_4_SEND_AT,
    FLD_DAY_6_ESCALATE_AT,
    STATUS_ACTIVE,
    STAGE_DAY_0_SENT,
    STAGE_DAY_2_SENT,
    STAGE_DAY_4_SENT,
    advance_ladder,
    _parse_iso,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("=== Follow-up ladder cron starting ===")

    if not config.AIRTABLE_API_KEY or not config.AIRTABLE_BASE_ID:
        logger.error("Missing Airtable credentials — aborting")
        return

    api = Api(config.AIRTABLE_API_KEY)
    table = api.table(config.AIRTABLE_BASE_ID, LADDER_TABLE_ID)

    now_et = datetime.now(ET)

    try:
        active = table.all(formula=f"{{{FLD_STATUS}}} = '{STATUS_ACTIVE}'")
    except Exception as exc:
        logger.error("Failed to list active ladders: %s", exc)
        return

    logger.info("Active ladders: %d", len(active))

    advanced = 0
    skipped = 0
    for row in active:
        f = row.get("fields", {})
        stage = f.get("Stage") or f.get(FLD_STAGE)

        due = False
        if stage == STAGE_DAY_0_SENT:
            t = _parse_iso(f.get("Day 2 Send At") or f.get(FLD_DAY_2_SEND_AT))
            due = t is not None and now_et >= t
        elif stage == STAGE_DAY_2_SENT:
            t = _parse_iso(f.get("Day 4 Send At") or f.get(FLD_DAY_4_SEND_AT))
            due = t is not None and now_et >= t
        elif stage == STAGE_DAY_4_SENT:
            t = _parse_iso(f.get("Day 6 Escalate At") or f.get(FLD_DAY_6_ESCALATE_AT))
            due = t is not None and now_et >= t

        if not due:
            skipped += 1
            continue

        try:
            if advance_ladder(row["id"], api=api, now_et=now_et):
                advanced += 1
        except Exception as exc:
            logger.error("Failed to advance ladder %s: %s", row.get("id"), exc)

    logger.info("=== Follow-up ladder cron complete — advanced=%d skipped=%d ===", advanced, skipped)


if __name__ == "__main__":
    main()
