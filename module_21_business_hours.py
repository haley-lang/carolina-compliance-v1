"""Business-hours utility — single source of truth for timing rules.

Business hours: Monday–Friday, 8:00am–6:00pm America/New_York.

Two event-type rules:

IMMEDIATE-SEND (Day 0 initial sends, bounce retries):
  - If target datetime is within business hours → return target.
  - If target is before 8am on a business day → return that day at 8am.
  - Else (after 6pm or on a weekend) → return next business day at 8am.

CADENCE-SEND (Day 2, Day 4, Day 6):
  - add_business_days(start, N) steps N weekday-only days forward from start
    and locks the result to 08:00 ET. Weekends do not count.

  Worked examples (Mon–Fri Day 0 → Day 2 / Day 4 / Day 6):
    Day 0 Mon → Wed, Fri, Tue (next week)
    Day 0 Tue → Thu, Mon (next week), Wed (next week)
    Day 0 Wed → Fri, Tue (next week), Thu (next week)
    Day 0 Thu → Mon (next week), Wed (next week), Fri (next week)
    Day 0 Fri → Tue (next week), Thu (next week), Mon (week after)

No federal holiday handling in v1.
"""

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")

BUSINESS_DAYS = {0, 1, 2, 3, 4}
BUSINESS_HOURS_START = 8
BUSINESS_HOURS_END = 18


def _ensure_et(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def is_business_hours(dt: datetime = None) -> bool:
    t = _ensure_et(dt) if dt else datetime.now(ET)
    return (
        t.weekday() in BUSINESS_DAYS
        and BUSINESS_HOURS_START <= t.hour < BUSINESS_HOURS_END
    )


def next_business_day_8am(from_date) -> datetime:
    """Returns 08:00 ET on the next business day strictly after from_date."""
    if isinstance(from_date, datetime):
        d = _ensure_et(from_date).date()
    else:
        d = from_date
    d = d + timedelta(days=1)
    while d.weekday() not in BUSINESS_DAYS:
        d = d + timedelta(days=1)
    return datetime(d.year, d.month, d.day, BUSINESS_HOURS_START, 0, 0, tzinfo=ET)


def send_now_or_defer(target_dt: datetime = None) -> datetime:
    """Immediate-send rule. Returns ET datetime."""
    t = _ensure_et(target_dt) if target_dt else datetime.now(ET)

    if is_business_hours(t):
        return t

    # Before 8am on a business day → snap forward to today's 8am
    if t.weekday() in BUSINESS_DAYS and t.hour < BUSINESS_HOURS_START:
        return t.replace(hour=BUSINESS_HOURS_START, minute=0, second=0, microsecond=0)

    # After 6pm on a business day, or any time on a weekend → next business day 8am
    return next_business_day_8am(t.date())


def add_business_days(start: datetime, n: int) -> datetime:
    """Cadence rule — step N weekday-only days forward from start, lock to 08:00 ET.

    Weekends do not count toward N. The start datetime's time-of-day is
    discarded; only the date is used as the anchor.
    """
    start = _ensure_et(start)
    target_date = start.date()
    days_added = 0
    while days_added < n:
        target_date = target_date + timedelta(days=1)
        if target_date.weekday() in BUSINESS_DAYS:
            days_added += 1
    return datetime(
        target_date.year, target_date.month, target_date.day,
        BUSINESS_HOURS_START, 0, 0, tzinfo=ET,
    )
