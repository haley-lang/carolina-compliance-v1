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
    def test_day_0_mon_8am_day_2_wed_8am(self):
        # "Day 0 sends Mon 8am → Day 2 = Wed 8am"
        day_0 = _et(2025, 3, 3, 8)
        assert add_business_days(day_0, 2) == _et(2025, 3, 5, 8)

    def test_day_4_thu_8am_day_6_snaps_sat_to_mon_8am(self):
        # "Day 4 sends Thu 8am → Day 6 target Sat → Mon 8am"
        day_4 = _et(2025, 3, 6, 8)   # Thu
        assert add_business_days(day_4, 2) == _et(2025, 3, 10, 8)   # Mon

    def test_day_0_fri_5pm_day_2_snaps_sun_to_mon_8am(self):
        # "Cert processes Fri 5pm → Day 0 sends Fri 5pm; Day 2 target Sun → Mon 8am"
        day_0 = _et(2025, 3, 7, 17)   # Fri 5pm
        assert add_business_days(day_0, 2) == _et(2025, 3, 10, 8)   # Mon 8am


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
