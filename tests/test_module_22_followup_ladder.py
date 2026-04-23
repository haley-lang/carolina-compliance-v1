"""Integration tests for module_22_followup_ladder.

Mocks Airtable with an in-memory table factory (from conftest) and exercises
the state machine: create, advance, terminate, bounce, halt, response-match.
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

import module_22_followup_ladder as m22
from module_22_followup_ladder import (
    LADDER_TYPE_NONCOMPLIANCE,
    LADDER_TYPE_SHORT_DATED,
    STAGE_DAY_0_QUEUED,
    STAGE_DAY_0_SENT,
    STAGE_DAY_2_SENT,
    STAGE_DAY_4_SENT,
    STAGE_DAY_6_ESCALATED,
    STAGE_RESPONDED,
    STAGE_RESOLVED,
    STAGE_BOUNCED_ESCALATED,
    STATUS_ACTIVE,
    STATUS_TERMINAL,
    EQ_TYPE_DAY_0,
    EQ_TYPE_DAY_2,
    EQ_TYPE_DAY_0_RETRY,
    DASHBOARD_APPROVAL_PENDING,
    normalize_for_matching,
    parse_body_reply_to,
    create_ladder,
    terminate_ladder,
    check_response_match,
    halt_ladders_for_vendor,
    handle_bounce,
    advance_ladder,
    generate_ladder_id,
    record_send_success,
    LADDER_TABLE_ID,
    EMAIL_QUEUE_TABLE_ID,
    VENDORS_TABLE_ID,
    CLIENTS_TABLE_ID,
    CERTS_TABLE_ID,
    FLD_LADDER_ID,
    FLD_STAGE,
    FLD_STATUS,
    FLD_NAMED_INSURED,
    FLD_ORIGINAL_SENDER,
    FLD_BODY_REPLY_TO,
    FLD_PDF_CONTACT_EMAIL,
    FLD_VENDOR_LINK,
    FLD_CLIENT_LINK,
    FLD_CERTIFICATE_LINK,
    FLD_DAY_0_SENT_AT,
    FLD_DAY_2_SEND_AT,
    FLD_DAY_2_SENT_AT,
    FLD_DAY_4_SEND_AT,
    FLD_DAY_6_ESCALATE_AT,
    FLD_BOUNCE_COUNT,
    FLD_CREATED_AT,
    FLD_DASHBOARD_APPROVAL,
    FLD_TERMINATION_REASON,
)


ET = ZoneInfo("America/New_York")


# ── Table factory shared across tests ────────────────────────────────────────

class FakeTable:
    """Minimal in-memory Airtable table."""

    def __init__(self, table_id):
        self.table_id = table_id
        self.records = {}
        self._counter = 0

    def create(self, fields, typecast=False):
        self._counter += 1
        rec_id = f"rec{self.table_id[-6:]}{self._counter:04d}"
        row = {"id": rec_id, "fields": dict(fields), "createdTime": datetime.utcnow().isoformat()}
        self.records[rec_id] = row
        return row

    def update(self, rec_id, fields, typecast=False):
        if rec_id not in self.records:
            raise KeyError(rec_id)
        self.records[rec_id]["fields"].update(fields)
        return self.records[rec_id]

    def get(self, rec_id):
        return self.records[rec_id]

    def all(self, formula=None, fields=None, max_records=None, **kwargs):
        rows = list(self.records.values())
        if formula:
            rows = [r for r in rows if _evaluate_formula(formula, r["fields"])]
        if max_records:
            rows = rows[:max_records]
        return rows


def _evaluate_formula(formula, fields):
    """Very loose pseudo-evaluator. Handles the formulas module_22 actually
    emits: {FieldName} = 'value', AND(...), OR(...), FIND('X', ARRAYJOIN({Field})),
    and the 'FIND(prefix, {Field}) = 1' case used by generate_ladder_id.
    """
    import re as _re
    f = formula.strip()

    # Handle literal "FIND('X', {field}) = 1" used by generate_ladder_id
    m = _re.match(r"FIND\('([^']*)', \{([^}]+)\}\) = 1$", f)
    if m:
        needle, field = m.group(1), m.group(2)
        val = str(fields.get(field, ""))
        return val.startswith(needle)

    def _eval(expr):
        expr = expr.strip()
        if expr.startswith("AND(") and expr.endswith(")"):
            inner = expr[4:-1]
            return all(_eval(p) for p in _split_top_level(inner))
        if expr.startswith("OR(") and expr.endswith(")"):
            inner = expr[3:-1]
            return any(_eval(p) for p in _split_top_level(inner))
        m2 = _re.match(r"\{([^}]+)\}\s*=\s*'([^']*)'$", expr)
        if m2:
            return str(fields.get(m2.group(1), "")) == m2.group(2)
        m3 = _re.match(r"FIND\('([^']*)', ARRAYJOIN\(\{([^}]+)\}\)\)$", expr)
        if m3:
            needle, field = m3.group(1), m3.group(2)
            val = fields.get(field, [])
            if isinstance(val, list):
                joined = ",".join(str(v) for v in val)
            else:
                joined = str(val)
            return needle in joined
        m4 = _re.match(r"\{([^}]+)\}\s*=\s*TRUE\(\)$", expr)
        if m4:
            return bool(fields.get(m4.group(1)))
        return False

    def _split_top_level(s):
        parts, depth, cur = [], 0, ""
        for ch in s:
            if ch == "(":
                depth += 1
                cur += ch
            elif ch == ")":
                depth -= 1
                cur += ch
            elif ch == "," and depth == 0:
                parts.append(cur.strip())
                cur = ""
            else:
                cur += ch
        if cur.strip():
            parts.append(cur.strip())
        return parts

    return _eval(f)


@pytest.fixture
def fake_tables():
    """Returns a dict of table_id → FakeTable, plus a mock Api proxying .table()."""
    tables = {
        LADDER_TABLE_ID:      FakeTable(LADDER_TABLE_ID),
        EMAIL_QUEUE_TABLE_ID: FakeTable(EMAIL_QUEUE_TABLE_ID),
        VENDORS_TABLE_ID:     FakeTable(VENDORS_TABLE_ID),
        CLIENTS_TABLE_ID:     FakeTable(CLIENTS_TABLE_ID),
        CERTS_TABLE_ID:       FakeTable(CERTS_TABLE_ID),
    }

    api = Mock()
    api.table.side_effect = lambda base_id, tid: tables[tid]
    return tables, api


# ── Pure helpers ─────────────────────────────────────────────────────────────

class TestNormalization:
    def test_strips_llc(self):
        assert normalize_for_matching("Acme Contractors, LLC") == "acme contractors"

    def test_strips_inc_and_punctuation(self):
        assert normalize_for_matching("Beta Builders, Inc.") == "beta builders"

    def test_handles_empty(self):
        assert normalize_for_matching("") == ""
        assert normalize_for_matching(None) == ""

    def test_case_insensitive(self):
        assert normalize_for_matching("ACME CORP") == normalize_for_matching("acme corp")


class TestParseBodyReplyTo:
    def test_extracts_reply_to(self):
        body = "Please reply to: agent@example.com for any questions."
        assert parse_body_reply_to(body) == "agent@example.com"

    def test_no_match_returns_none(self):
        assert parse_body_reply_to("no address here") is None

    def test_handles_empty(self):
        assert parse_body_reply_to("") is None


# ── generate_ladder_id ───────────────────────────────────────────────────────

class TestGenerateLadderId:
    def test_first_of_type_starts_at_00001(self, fake_tables):
        tables, api = fake_tables
        result = generate_ladder_id(LADDER_TYPE_NONCOMPLIANCE, api=api)
        assert result == "LADDER-NONCOMP-00001"

    def test_increments_max_existing(self, fake_tables):
        tables, api = fake_tables
        tables[LADDER_TABLE_ID].records["rec1"] = {
            "id": "rec1",
            "fields": {FLD_LADDER_ID: "LADDER-NONCOMP-00007"},
        }
        result = generate_ladder_id(LADDER_TYPE_NONCOMPLIANCE, api=api)
        assert result == "LADDER-NONCOMP-00008"

    def test_prefixes_are_separate_sequences(self, fake_tables):
        tables, api = fake_tables
        tables[LADDER_TABLE_ID].records["r1"] = {
            "id": "r1", "fields": {FLD_LADDER_ID: "LADDER-NONCOMP-00005"},
        }
        assert generate_ladder_id(LADDER_TYPE_SHORT_DATED, api=api) == "LADDER-SHORT-DATED-00001"
        assert generate_ladder_id(LADDER_TYPE_NONCOMPLIANCE, api=api) == "LADDER-NONCOMP-00006"


# ── create_ladder ────────────────────────────────────────────────────────────

class TestCreateLadder:
    def test_creates_ladder_and_day_0_eq(self, fake_tables, monkeypatch):
        tables, api = fake_tables
        # Pin "now" so Day 0 send computes deterministically (Mon 2pm ET)
        fixed_now = datetime(2025, 3, 3, 14, 0, tzinfo=ET)
        monkeypatch.setattr(m22, "_now_et", lambda: fixed_now)
        monkeypatch.setattr(m22, "send_now_or_defer", lambda t=None: fixed_now)
        monkeypatch.setattr(m22, "add_business_days",
                            lambda start, n: start + timedelta(days=n))

        result = create_ladder(
            cert_id="recCert123",
            ladder_type=LADDER_TYPE_NONCOMPLIANCE,
            email_subject="Documentation Update Needed: Acme — Smith GC",
            email_body="Hi Acme, your COI has deficiencies...",
            original_sender="acme@example.com",
            pdf_contact_email="agent@broker.com",
            vendor_id="recV1",
            client_id="recC1",
            named_insured="Acme Contractors LLC",
            api=api,
        )

        assert result is not None
        assert result.ladder_id == "LADDER-NONCOMP-00001"

        ladder = tables[LADDER_TABLE_ID].records[result.ladder_record_id]["fields"]
        assert ladder[FLD_STAGE] == STAGE_DAY_0_QUEUED
        assert ladder[FLD_STATUS] == STATUS_ACTIVE
        assert ladder[FLD_ORIGINAL_SENDER] == "acme@example.com"
        assert ladder[FLD_PDF_CONTACT_EMAIL] == "agent@broker.com"
        assert ladder[FLD_VENDOR_LINK] == ["recV1"]
        assert ladder[FLD_CERTIFICATE_LINK] == ["recCert123"]
        assert ladder[FLD_BOUNCE_COUNT] == 0

        # Day 0 Email Queue row should exist
        eq_rows = list(tables[EMAIL_QUEUE_TABLE_ID].records.values())
        assert len(eq_rows) == 1
        eq = eq_rows[0]["fields"]
        assert eq["Primary Email"] == "acme@example.com"
        assert eq["Email Type"] == EQ_TYPE_DAY_0
        assert eq["Reminder Reason"] == f"LADDER:{result.ladder_record_id}"

    def test_create_ladder_rejects_missing_sender(self, fake_tables):
        tables, api = fake_tables
        result = create_ladder(
            cert_id=None,
            ladder_type=LADDER_TYPE_NONCOMPLIANCE,
            email_subject="x", email_body="y",
            original_sender="",
            api=api,
        )
        assert result is None


# ── Response detection ───────────────────────────────────────────────────────

class TestCheckResponseMatch:
    def _setup_ladder(self, tables):
        tables[LADDER_TABLE_ID].records["recL1"] = {
            "id": "recL1",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE,
                FLD_STAGE: STAGE_DAY_0_SENT,
                FLD_ORIGINAL_SENDER: "acme@example.com",
                FLD_NAMED_INSURED: "Acme Contractors LLC",
                FLD_VENDOR_LINK: ["recV1"],
                FLD_CREATED_AT: "2025-03-03T14:00:00Z",
            },
        }

    def test_matching_sender_and_named_insured_halts(self, fake_tables):
        tables, api = fake_tables
        self._setup_ladder(tables)
        hit = check_response_match(
            inbound_sender="Acme@example.com",
            inbound_subject="Re: Documentation Update Needed",
            inbound_body="Here's our renewal for Acme Contractors, we're working on it.",
            api=api,
        )
        assert hit == "recL1"

    def test_matching_sender_but_no_named_insured_reference_does_nothing(self, fake_tables):
        tables, api = fake_tables
        self._setup_ladder(tables)
        hit = check_response_match(
            inbound_sender="acme@example.com",
            inbound_subject="General question",
            inbound_body="Hey, unrelated follow-up on another matter.",
            api=api,
        )
        assert hit is None

    def test_unknown_sender_returns_none(self, fake_tables):
        tables, api = fake_tables
        self._setup_ladder(tables)
        hit = check_response_match(
            inbound_sender="stranger@elsewhere.com",
            inbound_subject="Re: Acme Contractors LLC",
            inbound_body="",
            api=api,
        )
        assert hit is None

    def test_multiple_matches_halts_oldest(self, fake_tables):
        tables, api = fake_tables
        tables[LADDER_TABLE_ID].records["recOld"] = {
            "id": "recOld",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE, FLD_STAGE: STAGE_DAY_0_SENT,
                FLD_ORIGINAL_SENDER: "x@example.com",
                FLD_NAMED_INSURED: "Acme LLC",
                FLD_CREATED_AT: "2025-01-01T12:00:00Z",
            },
        }
        tables[LADDER_TABLE_ID].records["recNew"] = {
            "id": "recNew",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE, FLD_STAGE: STAGE_DAY_0_SENT,
                FLD_ORIGINAL_SENDER: "x@example.com",
                FLD_NAMED_INSURED: "Acme LLC",
                FLD_CREATED_AT: "2025-03-01T12:00:00Z",
            },
        }
        hit = check_response_match(
            inbound_sender="x@example.com",
            inbound_subject="Re: Acme",
            inbound_body="Acme response",
            api=api,
        )
        assert hit == "recOld"


# ── halt_ladders_for_vendor ──────────────────────────────────────────────────

class TestHaltLaddersForVendor:
    def test_terminates_all_active_ladders_for_vendor(self, fake_tables):
        tables, api = fake_tables
        tables[LADDER_TABLE_ID].records["recA"] = {
            "id": "recA",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE, FLD_STAGE: STAGE_DAY_0_SENT,
                FLD_VENDOR_LINK: ["recV1"],
            },
        }
        tables[LADDER_TABLE_ID].records["recB"] = {
            "id": "recB",
            "fields": {
                FLD_STATUS: STATUS_TERMINAL, FLD_STAGE: STAGE_RESOLVED,
                FLD_VENDOR_LINK: ["recV1"],
            },
        }
        n = halt_ladders_for_vendor("recV1", "New compliant cert received", api=api)
        assert n == 1
        assert tables[LADDER_TABLE_ID].records["recA"]["fields"][FLD_STAGE] == STAGE_RESOLVED
        assert tables[LADDER_TABLE_ID].records["recA"]["fields"][FLD_STATUS] == STATUS_TERMINAL


# ── Bounce handling ──────────────────────────────────────────────────────────

class TestHandleBounce:
    def _setup(self, tables):
        tables[LADDER_TABLE_ID].records["recL1"] = {
            "id": "recL1",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE,
                FLD_STAGE: STAGE_DAY_0_SENT,
                FLD_ORIGINAL_SENDER: "acme@example.com",
                FLD_PDF_CONTACT_EMAIL: "agent@broker.com",
                FLD_BOUNCE_COUNT: 0,
                FLD_CREATED_AT: "2025-03-03T14:00:00Z",
                "Email Subject": "Subject",
                "Email Body": "Body",
            },
        }

    def test_first_bounce_on_original_sender_retries_to_pdf_contact(self, fake_tables, monkeypatch):
        tables, api = fake_tables
        self._setup(tables)
        fixed_now = datetime(2025, 3, 3, 15, 0, tzinfo=ET)
        monkeypatch.setattr(m22, "_now_et", lambda: fixed_now)
        monkeypatch.setattr(m22, "send_now_or_defer", lambda t=None: fixed_now + timedelta(hours=1))

        ladder_id = handle_bounce("acme@example.com", api=api)
        assert ladder_id == "recL1"

        ladder = tables[LADDER_TABLE_ID].records["recL1"]["fields"]
        assert ladder[FLD_BOUNCE_COUNT] == 1
        assert ladder[FLD_STATUS] == STATUS_ACTIVE  # not terminated yet

        eq_rows = list(tables[EMAIL_QUEUE_TABLE_ID].records.values())
        assert len(eq_rows) == 1
        retry = eq_rows[0]["fields"]
        assert retry["Primary Email"] == "agent@broker.com"
        assert retry["Email Type"] == EQ_TYPE_DAY_0_RETRY

    def test_second_bounce_escalates(self, fake_tables, monkeypatch):
        tables, api = fake_tables
        self._setup(tables)
        tables[LADDER_TABLE_ID].records["recL1"]["fields"][FLD_BOUNCE_COUNT] = 1
        monkeypatch.setattr(m22, "_now_et", lambda: datetime(2025, 3, 3, 15, 0, tzinfo=ET))

        ladder_id = handle_bounce("agent@broker.com", api=api)
        assert ladder_id == "recL1"

        ladder = tables[LADDER_TABLE_ID].records["recL1"]["fields"]
        assert ladder[FLD_STAGE] == STAGE_BOUNCED_ESCALATED
        assert ladder[FLD_STATUS] == STATUS_TERMINAL
        assert ladder[FLD_DASHBOARD_APPROVAL] == DASHBOARD_APPROVAL_PENDING

    def test_bounce_on_pdf_contact_escalates_immediately(self, fake_tables, monkeypatch):
        tables, api = fake_tables
        self._setup(tables)
        monkeypatch.setattr(m22, "_now_et", lambda: datetime(2025, 3, 3, 15, 0, tzinfo=ET))

        handle_bounce("agent@broker.com", api=api)
        ladder = tables[LADDER_TABLE_ID].records["recL1"]["fields"]
        assert ladder[FLD_STAGE] == STAGE_BOUNCED_ESCALATED
        assert ladder[FLD_STATUS] == STATUS_TERMINAL


# ── advance_ladder ───────────────────────────────────────────────────────────

class TestAdvanceLadder:
    def _setup_day_0_sent(self, tables, day_2_send_at):
        tables[LADDER_TABLE_ID].records["recL1"] = {
            "id": "recL1",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE,
                FLD_STAGE: STAGE_DAY_0_SENT,
                FLD_ORIGINAL_SENDER: "acme@example.com",
                FLD_BODY_REPLY_TO: "",
                FLD_PDF_CONTACT_EMAIL: "agent@broker.com",
                FLD_DAY_2_SEND_AT: day_2_send_at,
                FLD_DAY_4_SEND_AT: "2025-03-07T13:00:00Z",
                FLD_DAY_6_ESCALATE_AT: "2025-03-09T13:00:00Z",
                "Email Subject": "Subject",
                "Email Body": "Body",
                FLD_VENDOR_LINK: ["recV1"],
            },
        }

    def test_day_2_fires_when_due(self, fake_tables):
        tables, api = fake_tables
        day_2_past = "2025-03-05T13:00:00Z"
        self._setup_day_0_sent(tables, day_2_past)

        now = datetime(2025, 3, 5, 14, 0, tzinfo=ET)
        acted = advance_ladder("recL1", api=api, now_et=now)
        assert acted is True

        eq_rows = list(tables[EMAIL_QUEUE_TABLE_ID].records.values())
        assert len(eq_rows) == 1
        eq = eq_rows[0]["fields"]
        assert eq["Email Type"] == EQ_TYPE_DAY_2
        assert eq["Primary Email"] == "acme@example.com"
        assert "agent@broker.com" in eq.get("CC Emails", [])
        assert eq["Body"].startswith("Following up on the below")

    def test_day_2_does_not_fire_before_send_at(self, fake_tables):
        tables, api = fake_tables
        day_2_future = "2025-03-10T13:00:00Z"
        self._setup_day_0_sent(tables, day_2_future)

        now = datetime(2025, 3, 5, 14, 0, tzinfo=ET)
        assert advance_ladder("recL1", api=api, now_et=now) is False

    def test_day_6_escalation_queues_haley_approval(self, fake_tables):
        tables, api = fake_tables
        tables[LADDER_TABLE_ID].records["recL1"] = {
            "id": "recL1",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE,
                FLD_STAGE: STAGE_DAY_4_SENT,
                FLD_ORIGINAL_SENDER: "acme@example.com",
                FLD_DAY_0_SENT_AT: "2025-03-03T13:00:00Z",
                FLD_DAY_2_SENT_AT: "2025-03-05T13:00:00Z",
                "Day 4 Sent At": "2025-03-07T13:00:00Z",
                FLD_DAY_6_ESCALATE_AT: "2025-03-09T13:00:00Z",
                "Email Subject": "Subject",
                "Email Body": "Body",
                FLD_VENDOR_LINK: ["recV1"],
                FLD_CLIENT_LINK: ["recC1"],
            },
        }
        tables[VENDORS_TABLE_ID].records["recV1"] = {
            "id": "recV1", "fields": {"Vendor Name": "Acme Contractors LLC"},
        }
        tables[CLIENTS_TABLE_ID].records["recC1"] = {
            "id": "recC1",
            "fields": {
                "fldIWXSLRJYAVRs3P": "Jane Smith",
                "fldmh1sYahgN5x6KQ": "primary@smithgc.com",
            },
        }

        now = datetime(2025, 3, 9, 14, 0, tzinfo=ET)
        acted = advance_ladder("recL1", api=api, now_et=now)
        assert acted is True

        ladder = tables[LADDER_TABLE_ID].records["recL1"]["fields"]
        assert ladder[FLD_STAGE] == STAGE_DAY_6_ESCALATED
        assert ladder[FLD_DASHBOARD_APPROVAL] == DASHBOARD_APPROVAL_PENDING

        # Haley-approval email queued
        approval_rows = [r for r in tables[EMAIL_QUEUE_TABLE_ID].records.values()
                         if "APPROVAL NEEDED" in r["fields"].get("Subject", "")]
        assert len(approval_rows) == 1
        approval = approval_rows[0]["fields"]
        # Config value for OWNER_EMAIL — import to avoid magic string
        import config as _cfg
        assert approval["Primary Email"] == _cfg.OWNER_EMAIL


# ── record_send_success ──────────────────────────────────────────────────────

class TestRecordSendSuccess:
    def test_day_0_stamps_day_0_sent_at_and_advances_stage(self, fake_tables):
        tables, api = fake_tables
        tables[LADDER_TABLE_ID].records["recL1"] = {
            "id": "recL1",
            "fields": {FLD_STATUS: STATUS_ACTIVE, FLD_STAGE: STAGE_DAY_0_QUEUED},
        }
        now = datetime(2025, 3, 3, 14, 0, tzinfo=ET)
        record_send_success("recL1", EQ_TYPE_DAY_0, sent_at=now, api=api)
        fields = tables[LADDER_TABLE_ID].records["recL1"]["fields"]
        assert fields[FLD_STAGE] == STAGE_DAY_0_SENT
        assert fields[FLD_DAY_0_SENT_AT].startswith("2025-03-03T")

    def test_day_0_retry_does_not_advance_stage(self, fake_tables):
        tables, api = fake_tables
        tables[LADDER_TABLE_ID].records["recL1"] = {
            "id": "recL1",
            "fields": {
                FLD_STATUS: STATUS_ACTIVE,
                FLD_STAGE: STAGE_DAY_0_SENT,
                FLD_DAY_0_SENT_AT: "2025-03-03T13:00:00Z",
            },
        }
        record_send_success("recL1", EQ_TYPE_DAY_0_RETRY, api=api)
        fields = tables[LADDER_TABLE_ID].records["recL1"]["fields"]
        assert fields[FLD_STAGE] == STAGE_DAY_0_SENT  # unchanged
