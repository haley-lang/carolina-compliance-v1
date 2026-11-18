"""Tests for the Clear CCS match and Set CCS match controls.

Scenarios:
- Clearing ABS Insulating's stale alias drops it from ccs_vendor
- Setting M.Y. Company to M Y Construction Co Inc makes it ccs_vendor
- Coverage counts update after either action
"""
import os
import time
import uuid
import sqlite3
import json
from unittest.mock import patch, MagicMock

import pytest

os.environ.setdefault("REVIEW_PASSWORD", "test_review_pass")
os.environ.setdefault("ANTHROPIC_API_KEY", "test_key")
os.environ.setdefault("AIRTABLE_API_KEY", "test_airtable_key")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def tmp_db(monkeypatch, tmp_path):
    db_path = str(tmp_path / "audit_test.db")
    monkeypatch.setenv("AUDIT_DB_PATH", db_path)
    import importlib
    import audit_store as _store
    importlib.reload(_store)
    return _store


@pytest.fixture
def app(tmp_db):
    import review_app
    review_app.app.config["TESTING"] = True
    return review_app.app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def authed_client(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["logged_in_at"] = time.time()
        sess["csrf_token"] = "test_csrf_32chars_paddingpadding0"
    return client


CSRF = "test_csrf_32chars_paddingpadding0"


def _make_audit(tmp_db, client_id="recCLIENT1"):
    rec = tmp_db.create_audit(
        client_id=client_id,
        client_name="Test GC",
        coverage="GL",
        start_date="2025-01-01",
        end_date="2025-12-31",
    )
    return rec["id"]


def _insert_payment(tmp_db, audit_id, payee_raw, amount=10000.0):
    db_path = os.environ["AUDIT_DB_PATH"]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO payments (id, audit_id, file_id, payee_raw, amount,
               txn_date, excluded, created_at)
               VALUES (?,?,?,?,?,?,0,?)""",
            (str(uuid.uuid4()), audit_id, "fileX", payee_raw, amount, "2025-03-01", time.time()),
        )


def _post_json(authed_client, url, data):
    return authed_client.post(
        url,
        data=json.dumps({**data, "csrf_token": CSRF}),
        content_type="application/json",
    )


# ── Vendor list mock ───────────────────────────────────────────────────────────

VENDORS_ABS = [{"id": "recABS", "name": "ABS Insulating Company, Inc."}]
VENDORS_MY  = [{"id": "recMY",  "name": "M Y Construction Co Inc"}]


# ── Clear match ───────────────────────────────────────────────────────────────

TRUTEAM_PAYEE = "TruTeam Builder Services Group, Inc. d/b/a Pender & Pettus Insulating"


class TestClearMatch:
    def test_clear_match_drops_ccs_vendor(self, authed_client, tmp_db):
        """Clearing TruTeam's stale alias (pointing to ABS Insulating) drops it from ccs_vendor."""
        aid = _make_audit(tmp_db)
        _insert_payment(tmp_db, aid, TRUTEAM_PAYEE)

        # Seed the stale alias (simulates old 'Confirm all likely' click before the bug fix)
        tmp_db.upsert_alias(
            client_id="recCLIENT1",
            payee_raw=TRUTEAM_PAYEE,
            vendor_id="recABS",
            vendor_name="ABS Insulating Company, Inc.",
            confirmed=1,
        )
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1",
            payee_raw=TRUTEAM_PAYEE,
            vendor_id="recABS", vendor_name="ABS Insulating Company, Inc.",
            match_type="exact", payee_group="ccs_vendor", confirmed=1,
        )

        with patch("review_app._get_vendor_list_for_client", return_value=VENDORS_ABS):
            resp = _post_json(authed_client, f"/audits/{aid}/payees/clear-match",
                              {"payee_raw": TRUTEAM_PAYEE})

        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        # TruTeam vs ABS is only a possible match — must NOT stay ccs_vendor after alias cleared
        assert body["payee_group"] != "ccs_vendor", (
            f"Expected TruTeam to leave ccs_vendor after alias cleared; got {body['payee_group']!r}"
        )

    def test_clear_match_removes_alias_from_db(self, authed_client, tmp_db):
        """The alias row is deleted from the DB, not just the classification."""
        aid = _make_audit(tmp_db)
        _insert_payment(tmp_db, aid, TRUTEAM_PAYEE)
        tmp_db.upsert_alias(
            client_id="recCLIENT1",
            payee_raw=TRUTEAM_PAYEE,
            vendor_id="recABS",
            vendor_name="ABS Insulating Company, Inc.",
            confirmed=1,
        )

        with patch("review_app._get_vendor_list_for_client", return_value=VENDORS_ABS):
            _post_json(authed_client, f"/audits/{aid}/payees/clear-match",
                       {"payee_raw": TRUTEAM_PAYEE})

        alias = tmp_db.get_alias("recCLIENT1", TRUTEAM_PAYEE)
        assert alias is None, "Alias should have been deleted"

    def test_clear_match_requires_payee_raw(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = _post_json(authed_client, f"/audits/{aid}/payees/clear-match", {})
        assert resp.status_code == 400

    def test_clear_match_requires_login(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = _post_json(client, f"/audits/{aid}/payees/clear-match",
                          {"payee_raw": "ABS Insulating Company, Inc."})
        assert resp.status_code == 302


# ── Set match ─────────────────────────────────────────────────────────────────

class TestSetMatch:
    def test_set_match_promotes_to_ccs_vendor(self, authed_client, tmp_db):
        """Setting M.Y. Company → M Y Construction Co Inc makes it ccs_vendor."""
        aid = _make_audit(tmp_db)
        _insert_payment(tmp_db, aid, "M.Y. Company")
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1",
            payee_raw="M.Y. Company",
            payee_group="unclear", confirmed=0,
        )

        resp = _post_json(authed_client, f"/audits/{aid}/payees/set-match", {
            "payee_raw": "M.Y. Company",
            "vendor_id": "recMY",
            "vendor_name": "M Y Construction Co Inc",
        })

        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True

        cls = tmp_db.get_payee_classification(aid, "M.Y. Company")
        assert cls is not None
        assert cls["payee_group"] == "ccs_vendor"
        assert cls["match_type"] == "exact"
        assert cls["vendor_id"] == "recMY"
        assert cls["confirmed"] == 1

    def test_set_match_saves_alias(self, authed_client, tmp_db):
        """The confirmed alias is persisted so future audits pick it up automatically."""
        aid = _make_audit(tmp_db)
        _insert_payment(tmp_db, aid, "M.Y. Company")

        resp = _post_json(authed_client, f"/audits/{aid}/payees/set-match", {
            "payee_raw": "M.Y. Company",
            "vendor_id": "recMY",
            "vendor_name": "M Y Construction Co Inc",
        })

        assert resp.status_code == 200
        alias = tmp_db.get_alias("recCLIENT1", "M.Y. Company")
        assert alias is not None
        assert alias["vendor_id"] == "recMY"
        assert alias["confirmed"] == 1

    def test_set_match_requires_all_fields(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = _post_json(authed_client, f"/audits/{aid}/payees/set-match",
                          {"payee_raw": "M.Y. Company"})
        assert resp.status_code == 400

    def test_set_match_requires_login(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = _post_json(client, f"/audits/{aid}/payees/set-match", {
            "payee_raw": "M.Y. Company",
            "vendor_id": "recMY",
            "vendor_name": "M Y Construction Co Inc",
        })
        assert resp.status_code == 302


# ── Vendor list endpoint ───────────────────────────────────────────────────────

class TestAuditVendors:
    def test_returns_vendor_list(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        vendors = [
            {"id": "recA", "name": "Zeta Roofing"},
            {"id": "recB", "name": "Alpha Plumbing"},
        ]
        with patch("review_app._get_vendor_list_for_client", return_value=vendors):
            resp = authed_client.get(f"/audits/{aid}/vendors")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        names = [v["name"] for v in body["vendors"]]
        assert names == ["Alpha Plumbing", "Zeta Roofing"]  # sorted

    def test_requires_login(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = client.get(f"/audits/{aid}/vendors")
        assert resp.status_code == 302


# ── Coverage count updates ─────────────────────────────────────────────────────
# These tests verify the bucket logic directly via audit_buckets (pure functions)
# rather than the full /analysis route (which requires deep Airtable mocking).

class TestCoverageAfterMatchChange:
    def test_coverage_drops_after_clear_match(self, authed_client, tmp_db):
        """After clearing TruTeam's stale alias, its classification leaves ccs_vendor
        so the bucket module no longer counts it in sub_on_file/sub_with_gap."""
        import audit_buckets

        aid = _make_audit(tmp_db)
        _insert_payment(tmp_db, aid, TRUTEAM_PAYEE, 5000.0)

        # Seed stale alias + ccs_vendor classification
        tmp_db.upsert_alias(
            client_id="recCLIENT1",
            payee_raw=TRUTEAM_PAYEE,
            vendor_id="recABS", vendor_name="ABS Insulating Company, Inc.", confirmed=1,
        )
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1",
            payee_raw=TRUTEAM_PAYEE,
            vendor_id="recABS", vendor_name="ABS Insulating Company, Inc.",
            match_type="exact", payee_group="ccs_vendor", confirmed=1,
        )

        with patch("review_app._get_vendor_list_for_client", return_value=VENDORS_ABS):
            _post_json(authed_client, f"/audits/{aid}/payees/clear-match",
                       {"payee_raw": TRUTEAM_PAYEE})

        # Verify the classification was updated out of ccs_vendor
        cls = tmp_db.get_payee_classification(aid, TRUTEAM_PAYEE)
        assert cls is not None
        assert cls["payee_group"] != "ccs_vendor", (
            f"Classification should leave ccs_vendor after alias cleared; got {cls['payee_group']!r}"
        )

        # Verify bucket logic sees it as not ccs_vendor
        bucket = audit_buckets.payee_bucket(cls["payee_group"], "not_in_ccs")
        assert bucket not in ("sub_on_file", "sub_with_gap"), (
            f"TruTeam should not be in a sub bucket after clear; got bucket={bucket!r}"
        )

    def test_coverage_rises_after_set_match(self, authed_client, tmp_db):
        """After setting M.Y. Company → M Y Construction Co Inc, the classification
        becomes ccs_vendor so the bucket module counts it in sub_with_gap (no policies)."""
        import audit_buckets

        aid = _make_audit(tmp_db)
        _insert_payment(tmp_db, aid, "M.Y. Company", 8000.0)
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1",
            payee_raw="M.Y. Company",
            payee_group="unclear", confirmed=0,
        )

        resp = _post_json(authed_client, f"/audits/{aid}/payees/set-match", {
            "payee_raw": "M.Y. Company",
            "vendor_id": "recMY",
            "vendor_name": "M Y Construction Co Inc",
        })
        assert resp.status_code == 200

        cls = tmp_db.get_payee_classification(aid, "M.Y. Company")
        assert cls is not None
        assert cls["payee_group"] == "ccs_vendor"

        # Bucket logic: ccs_vendor with no policy on file → sub_with_gap
        bucket = audit_buckets.payee_bucket(cls["payee_group"], "not_in_ccs")
        assert bucket in ("sub_on_file", "sub_with_gap"), (
            f"M.Y. Company should be in a sub bucket after set-match; got bucket={bucket!r}"
        )
