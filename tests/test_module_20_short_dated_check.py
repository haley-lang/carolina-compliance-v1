"""Integration tests for module_20_short_dated_check."""

from datetime import date, datetime, timedelta
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

import module_20_short_dated_check as m20
from module_20_short_dated_check import (
    check_and_trigger_short_dated,
    CERTS_TABLE_ID,
    POLICIES_TABLE_ID,
    EXTRACTIONS_TABLE_ID,
    CLIENTS_TABLE_ID,
    VENDORS_TABLE_ID,
    COOLDOWN_DAYS,
)
from module_22_followup_ladder import (
    LADDER_TABLE_ID,
    EMAIL_QUEUE_TABLE_ID,
    LADDER_TYPE_SHORT_DATED,
    FLD_LADDER_TYPE,
    FLD_NAMED_INSURED,
)


# Reuse the FakeTable + api fixture from the module_22 tests by duplicating here
# (conftest isn't structured to share fixtures across modules without import gymnastics).

class FakeTable:
    def __init__(self, table_id):
        self.table_id = table_id
        self.records = {}
        self._counter = 0

    def create(self, fields, typecast=False):
        self._counter += 1
        rec_id = f"rec{self.table_id[-6:]}{self._counter:04d}"
        row = {"id": rec_id, "fields": dict(fields)}
        self.records[rec_id] = row
        return row

    def update(self, rec_id, fields, typecast=False):
        self.records[rec_id]["fields"].update(fields)
        return self.records[rec_id]

    def get(self, rec_id):
        return self.records[rec_id]

    def all(self, formula=None, fields=None, **kwargs):
        return list(self.records.values())


@pytest.fixture
def fake_tables():
    tables = {
        LADDER_TABLE_ID:      FakeTable(LADDER_TABLE_ID),
        EMAIL_QUEUE_TABLE_ID: FakeTable(EMAIL_QUEUE_TABLE_ID),
        CERTS_TABLE_ID:       FakeTable(CERTS_TABLE_ID),
        POLICIES_TABLE_ID:    FakeTable(POLICIES_TABLE_ID),
        EXTRACTIONS_TABLE_ID: FakeTable(EXTRACTIONS_TABLE_ID),
        CLIENTS_TABLE_ID:     FakeTable(CLIENTS_TABLE_ID),
        VENDORS_TABLE_ID:     FakeTable(VENDORS_TABLE_ID),
    }
    api = Mock()
    api.table.side_effect = lambda base_id, tid: tables[tid]
    return tables, api


def _seed_cert(tables, cert_id, policy_ids, extraction_ids=(), short_dated_flagged_at=None):
    fields = {
        "Named Insured": "Acme Contractors LLC",
        "Insurance Policies": list(policy_ids),
        "Vendor Link": ["recV1"],
        "Client Link": ["recC1"],
        "Incoming Extractions": list(extraction_ids),
    }
    if short_dated_flagged_at:
        fields["Short-Dated Flagged At"] = short_dated_flagged_at
    tables[CERTS_TABLE_ID].records[cert_id] = {"id": cert_id, "fields": fields}


def _seed_policy(tables, policy_id, days_until_expiration, policy_type="General Liability"):
    tables[POLICIES_TABLE_ID].records[policy_id] = {
        "id": policy_id,
        "fields": {
            "Policy Type": policy_type,
            "Policy Number": f"P-{policy_id[-3:]}",
            "Expiration Date": (date.today() + timedelta(days=days_until_expiration)).isoformat(),
            "Days Until Expiration": days_until_expiration,
        },
    }


def _seed_extraction(tables, ext_id, emails_list):
    tables[EXTRACTIONS_TABLE_ID].records[ext_id] = {
        "id": ext_id,
        "fields": {"Contact Emails": "\n".join(emails_list)},
    }


def _seed_client(tables, client_id="recC1", name="Smith GC"):
    tables[CLIENTS_TABLE_ID].records[client_id] = {
        "id": client_id, "fields": {"Client Name": name},
    }


class TestShortDatedTrigger:
    def test_triggers_ladder_when_policy_expires_within_30_days(self, fake_tables, monkeypatch):
        tables, api = fake_tables
        _seed_cert(tables, "recCertA", ["recP1"], extraction_ids=["recE1"])
        _seed_policy(tables, "recP1", days_until_expiration=15)
        _seed_extraction(tables, "recE1", ["agent@broker.com", "backup@broker.com"])
        _seed_client(tables)

        fixed_now = datetime(2025, 3, 3, 14, 0, tzinfo=ZoneInfo("America/New_York"))
        import module_22_followup_ladder as m22
        monkeypatch.setattr(m22, "_now_et", lambda: fixed_now)
        monkeypatch.setattr(m22, "send_now_or_defer", lambda t=None: fixed_now)
        monkeypatch.setattr(m22, "add_business_days",
                            lambda start, n: start + timedelta(days=n))

        ok = check_and_trigger_short_dated("recCertA", api=api)
        assert ok is True

        ladder_rows = list(tables[LADDER_TABLE_ID].records.values())
        assert len(ladder_rows) == 1
        ladder = ladder_rows[0]["fields"]
        assert ladder[FLD_LADDER_TYPE] == LADDER_TYPE_SHORT_DATED
        assert ladder[FLD_NAMED_INSURED] == "Acme Contractors LLC"

        # Cert stamped — module_20 writes field IDs; in real Airtable those
        # surface as the human field names, but the in-memory mock stores
        # whatever key the writer used.
        from module_20_short_dated_check import (
            FLD_CERT_HAS_SHORT_DATED, FLD_CERT_SHORT_DATED_FLAGGED_AT,
        )
        cert = tables[CERTS_TABLE_ID].records["recCertA"]["fields"]
        assert cert[FLD_CERT_HAS_SHORT_DATED] is True
        assert cert[FLD_CERT_SHORT_DATED_FLAGGED_AT] == date.today().isoformat()

    def test_no_trigger_when_all_policies_over_30_days(self, fake_tables):
        tables, api = fake_tables
        _seed_cert(tables, "recCertA", ["recP1"], extraction_ids=["recE1"])
        _seed_policy(tables, "recP1", days_until_expiration=90)
        _seed_extraction(tables, "recE1", ["agent@broker.com"])

        ok = check_and_trigger_short_dated("recCertA", api=api)
        assert ok is False
        assert len(tables[LADDER_TABLE_ID].records) == 0

    def test_no_trigger_when_policy_already_expired(self, fake_tables):
        tables, api = fake_tables
        _seed_cert(tables, "recCertA", ["recP1"], extraction_ids=["recE1"])
        _seed_policy(tables, "recP1", days_until_expiration=-5)
        _seed_extraction(tables, "recE1", ["agent@broker.com"])

        ok = check_and_trigger_short_dated("recCertA", api=api)
        assert ok is False

    def test_cooldown_suppresses_second_trigger_within_14_days(self, fake_tables):
        tables, api = fake_tables
        recent = (date.today() - timedelta(days=7)).isoformat()
        _seed_cert(tables, "recCertA", ["recP1"],
                   extraction_ids=["recE1"],
                   short_dated_flagged_at=recent)
        _seed_policy(tables, "recP1", days_until_expiration=15)
        _seed_extraction(tables, "recE1", ["agent@broker.com"])

        ok = check_and_trigger_short_dated("recCertA", api=api)
        assert ok is False
        assert len(tables[LADDER_TABLE_ID].records) == 0

    def test_cooldown_releases_after_14_days(self, fake_tables, monkeypatch):
        tables, api = fake_tables
        old = (date.today() - timedelta(days=COOLDOWN_DAYS + 1)).isoformat()
        _seed_cert(tables, "recCertA", ["recP1"],
                   extraction_ids=["recE1"],
                   short_dated_flagged_at=old)
        _seed_policy(tables, "recP1", days_until_expiration=15)
        _seed_extraction(tables, "recE1", ["agent@broker.com"])
        _seed_client(tables)

        import module_22_followup_ladder as m22
        fixed_now = datetime(2025, 3, 3, 14, 0, tzinfo=ZoneInfo("America/New_York"))
        monkeypatch.setattr(m22, "_now_et", lambda: fixed_now)
        monkeypatch.setattr(m22, "send_now_or_defer", lambda t=None: fixed_now)
        monkeypatch.setattr(m22, "add_business_days",
                            lambda start, n: start + timedelta(days=n))

        ok = check_and_trigger_short_dated("recCertA", api=api)
        assert ok is True

    def test_no_trigger_when_no_contact_emails(self, fake_tables):
        tables, api = fake_tables
        _seed_cert(tables, "recCertA", ["recP1"], extraction_ids=["recE1"])
        _seed_policy(tables, "recP1", days_until_expiration=15)
        _seed_extraction(tables, "recE1", [])

        ok = check_and_trigger_short_dated("recCertA", api=api)
        assert ok is False

    def test_email_body_uses_documentation_framing(self, fake_tables, monkeypatch):
        """Legal-framing guard: body must say 'coverage shown on this certificate'
        and 'documentation gap', never 'insurance expires' or 'policy expires'."""
        tables, api = fake_tables
        _seed_cert(tables, "recCertA", ["recP1"], extraction_ids=["recE1"])
        _seed_policy(tables, "recP1", days_until_expiration=15)
        _seed_extraction(tables, "recE1", ["agent@broker.com"])
        _seed_client(tables)

        import module_22_followup_ladder as m22
        monkeypatch.setattr(m22, "_now_et",
                            lambda: datetime(2025, 3, 3, 14, 0, tzinfo=ZoneInfo("America/New_York")))
        monkeypatch.setattr(m22, "send_now_or_defer",
                            lambda t=None: datetime(2025, 3, 3, 14, 0, tzinfo=ZoneInfo("America/New_York")))
        monkeypatch.setattr(m22, "add_business_days",
                            lambda start, n: start + timedelta(days=n))

        check_and_trigger_short_dated("recCertA", api=api)
        eq = list(tables[EMAIL_QUEUE_TABLE_ID].records.values())[0]["fields"]
        body = eq["Body"].lower()
        assert "coverage shown on this certificate" in body
        assert "documentation gap" in body
        assert "your insurance expires" not in body
        assert "policy expires" not in body
