"""Unit tests for module_21_business_hours.

Covers every worked example from the build brief's business-hours section.
No freezegun required — all functions accept explicit datetimes.
"""

from datetime import datetime, date
from zoneinfo import ZoneInfo

import pytest

from module_21_business_hours import (
    ET,
    is_business_hours,
    send_now_or_defer,
    next_business_day_8am,
    add_business_days,
)


def _et(y, m, d, h=0, minute=0):
    return datetime(y, m, d, h, minute, tzinfo=ET)


# ── is_business_hours ────────────────────────────────────────────────────────

class TestIsBusinessHours:
    def test_mon_2pm_in_hours(self):
        # Mon Mar 3 2025 2pm ET
        assert is_business_hours(_et(2025, 3, 3, 14)) is True

    def test_mon_7am_before_hours(self):
        assert is_business_hours(_et(2025, 3, 3, 7)) is False

    def test_mon_8am_boundary_in(self):
        assert is_business_hours(_et(2025, 3, 3, 8)) is True

    def test_mon_6pm_boundary_out(self):
        assert is_business_hours(_et(2025, 3, 3, 18)) is False

    def test_mon_5_59pm_in(self):
        assert is_business_hours(_et(2025, 3, 3, 17, 59)) is True

    def test_sat_10am_weekend(self):
        # Sat Mar 8 2025
        assert is_business_hours(_et(2025, 3, 8, 10)) is False

    def test_sun_noon_weekend(self):
        assert is_business_hours(_et(2025, 3, 9, 12)) is False

    def test_naive_datetime_assumed_et(self):
        # Naive Mon 2pm → treated as ET
        assert is_business_hours(datetime(2025, 3, 3, 14)) is True


# ── send_now_or_defer (Day 0 immediate-send + bounce retries) ────────────────

class TestSendNowOrDefer:
    """Covers every worked example from the brief."""

    def test_cert_processes_mon_2pm_sends_mon_2pm(self):
        # "Cert processes Mon 2pm → Day 0 sends Mon 2pm immediately"
        t = _et(2025, 3, 3, 14)
        assert send_now_or_defer(t) == t

    def test_cert_processes_mon_7am_sends_mon_8am(self):
        # "Cert processes Mon 7am → Day 0 sends Mon 8am"
        t = _et(2025, 3, 3, 7)
        assert send_now_or_defer(t) == _et(2025, 3, 3, 8)

    def test_cert_processes_sat_10am_sends_mon_8am(self):
        # "Cert processes Sat 10am → Day 0 sends Mon 8am"
        t = _et(2025, 3, 8, 10)   # Saturday
        assert send_now_or_defer(t) == _et(2025, 3, 10, 8)  # Monday 8am

    def test_cert_processes_fri_5pm_sends_fri_5pm(self):
        # "Cert processes Fri 5pm → Day 0 sends Fri 5pm immediately"
        t = _et(2025, 3, 7, 17)   # Friday 5pm
        assert send_now_or_defer(t) == t

    def test_bounce_retry_mon_4pm_sends_mon_4pm(self):
        # "Bounce received Mon 3pm → retry Mon 4pm"
        # Caller computes target = bounce_time + 1hr = Mon 4pm.
        t = _et(2025, 3, 3, 16)
        assert send_now_or_defer(t) == t

    def test_bounce_retry_fri_630pm_defers_to_mon_8am(self):
        # "Bounce received Fri 5:30pm → retry Mon 8am (1hr would be 6:30pm)"
        t = _et(2025, 3, 7, 18, 30)   # Fri 6:30pm
        assert send_now_or_defer(t) == _et(2025, 3, 10, 8)

    def test_bounce_retry_tue_7am_snaps_to_tue_8am(self):
        # "Bounce received Tue 6am → retry Tue 8am (before hours)"
        # Caller computes target = Tue 6am + 1hr = Tue 7am.
        t = _et(2025, 3, 4, 7)
        assert send_now_or_defer(t) == _et(2025, 3, 4, 8)

    def test_bounce_retry_mon_630pm_defers_to_tue_8am(self):
        # "Bounce received Mon 5:30pm → retry Tue 8am (1hr would be 6:30pm, after hours)"
        t = _et(2025, 3, 3, 18, 30)
        assert send_now_or_defer(t) == _et(2025, 3, 4, 8)


# ── Cadence (Day 2, Day 4, Day 6) ────────────────────────────────────────────

class TestAddBusinessDays:
    """True business-day arithmetic: weekends do not count toward N.

    Reference week: Mon 2025-03-03 through Fri 2025-03-07 — clean week, no
    federal holidays. Day N targets land in the following week(s) per the
    table below.

    Day 0      | +2 BD          | +4 BD          | +6 BD
    -----------|----------------|----------------|----------------
    Mon 03-03  | Wed 03-05      | Fri 03-07      | Tue 03-11
    Tue 03-04  | Thu 03-06      | Mon 03-10      | Wed 03-12
    Wed 03-05  | Fri 03-07      | Tue 03-11      | Thu 03-13
    Thu 03-06  | Mon 03-10      | Wed 03-12      | Fri 03-14
    Fri 03-07  | Tue 03-11      | Thu 03-13      | Mon 03-17
    """

    # Day 0 = Monday
    def test_mon_day_0_plus_2(self):
        assert add_business_days(_et(2025, 3, 3, 8), 2) == _et(2025, 3, 5, 8)

    def test_mon_day_0_plus_4(self):
        assert add_business_days(_et(2025, 3, 3, 8), 4) == _et(2025, 3, 7, 8)

    def test_mon_day_0_plus_6(self):
        assert add_business_days(_et(2025, 3, 3, 8), 6) == _et(2025, 3, 11, 8)

    # Day 0 = Tuesday
    def test_tue_day_0_plus_2(self):
        assert add_business_days(_et(2025, 3, 4, 10), 2) == _et(2025, 3, 6, 8)

    def test_tue_day_0_plus_4(self):
        assert add_business_days(_et(2025, 3, 4, 10), 4) == _et(2025, 3, 10, 8)

    def test_tue_day_0_plus_6(self):
        assert add_business_days(_et(2025, 3, 4, 10), 6) == _et(2025, 3, 12, 8)

    # Day 0 = Wednesday
    def test_wed_day_0_plus_2(self):
        assert add_business_days(_et(2025, 3, 5, 14), 2) == _et(2025, 3, 7, 8)

    def test_wed_day_0_plus_4(self):
        assert add_business_days(_et(2025, 3, 5, 14), 4) == _et(2025, 3, 11, 8)

    def test_wed_day_0_plus_6(self):
        assert add_business_days(_et(2025, 3, 5, 14), 6) == _et(2025, 3, 13, 8)

    # Day 0 = Thursday — the case that exposed the production bug
    def test_thu_day_0_plus_2(self):
        assert add_business_days(_et(2025, 3, 6, 10, 13), 2) == _et(2025, 3, 10, 8)

    def test_thu_day_0_plus_4(self):
        assert add_business_days(_et(2025, 3, 6, 10, 13), 4) == _et(2025, 3, 12, 8)

    def test_thu_day_0_plus_6(self):
        assert add_business_days(_et(2025, 3, 6, 10, 13), 6) == _et(2025, 3, 14, 8)

    # Day 0 = Friday
    def test_fri_day_0_plus_2(self):
        # Fri 5pm + 2 BD = Tue. Old test expected Mon (calendar-snap-from-Sun).
        assert add_business_days(_et(2025, 3, 7, 17), 2) == _et(2025, 3, 11, 8)

    def test_fri_day_0_plus_4(self):
        assert add_business_days(_et(2025, 3, 7, 17), 4) == _et(2025, 3, 13, 8)

    def test_fri_day_0_plus_6(self):
        assert add_business_days(_et(2025, 3, 7, 17), 6) == _et(2025, 3, 17, 8)

    # Time-of-day on `start` is discarded; only the date anchors the count.
    def test_start_time_of_day_is_irrelevant(self):
        morning = add_business_days(_et(2025, 3, 6, 6, 30), 2)
        evening = add_business_days(_et(2025, 3, 6, 23, 45), 2)
        assert morning == evening == _et(2025, 3, 10, 8)

    # Cadence steps are independent: +4 BD ≠ +2 BD chained twice from
    # weekday-anchored Day 0, but should equal +2 BD from the +2 result
    # when that result is itself a business day.
    def test_chained_consistency_from_thursday(self):
        day_0 = _et(2025, 3, 6, 10, 13)   # Thu
        day_2 = add_business_days(day_0, 2)        # Mon 03-10 8am
        day_4_direct = add_business_days(day_0, 4)
        day_4_chained = add_business_days(day_2, 2)
        assert day_4_direct == day_4_chained == _et(2025, 3, 12, 8)


# ── next_business_day_8am ────────────────────────────────────────────────────

class TestNextBusinessDay8am:
    def test_from_mon_returns_tue_8am(self):
        d = date(2025, 3, 3)
        assert next_business_day_8am(d) == _et(2025, 3, 4, 8)

    def test_from_fri_returns_mon_8am(self):
        d = date(2025, 3, 7)
        assert next_business_day_8am(d) == _et(2025, 3, 10, 8)

    def test_from_sat_returns_mon_8am(self):
        d = date(2025, 3, 8)
        assert next_business_day_8am(d) == _et(2025, 3, 10, 8)

    def test_accepts_datetime_input(self):
        dt = _et(2025, 3, 7, 23, 59)   # Fri 11:59pm
        assert next_business_day_8am(dt) == _et(2025, 3, 10, 8)


# ── DST boundary sanity ──────────────────────────────────────────────────────

class TestDstBoundaries:
    def test_spring_forward_2025(self):
        # DST begins Sun Mar 9 2025 at 2am ET.
        # Mon Mar 10 8am ET should be unambiguous.
        d = date(2025, 3, 7)
        result = next_business_day_8am(d)
        assert result == _et(2025, 3, 10, 8)
        assert result.utcoffset().total_seconds() == -4 * 3600  # EDT

    def test_fall_back_2025(self):
        # DST ends Sun Nov 2 2025 at 2am ET.
        d = date(2025, 10, 31)   # Friday
        result = next_business_day_8am(d)
        assert result == _et(2025, 11, 3, 8)   # Monday after fall-back
        assert result.utcoffset().total_seconds() == -5 * 3600  # EST
