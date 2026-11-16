"""Tests for the client-facing payee review flow.

Tokens are now stored in Airtable (not SQLite). Tests mock _get_review_token /
_get_active_review_token_for_audit / _create_review_token at the review_app level
rather than inserting into audit.db.
"""
import os
import time
import uuid
import sqlite3
from unittest.mock import MagicMock, patch, call

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


def _make_audit(tmp_db, client_id="recCLIENT1", client_name="Test GC"):
    rec = tmp_db.create_audit(
        client_id=client_id,
        client_name=client_name,
        coverage="GL",
        start_date="2025-01-01",
        end_date="2025-12-31",
    )
    return rec["id"]


def _insert_payments(tmp_db, audit_id, rows):
    """rows = [(payee_raw, amount, txn_date), ...]"""
    db_path = os.environ["AUDIT_DB_PATH"]
    with sqlite3.connect(db_path) as conn:
        for payee_raw, amount, txn_date in rows:
            conn.execute(
                """INSERT INTO payments (id, audit_id, file_id, payee_raw, amount,
                   txn_date, excluded, created_at)
                   VALUES (?,?,?,?,?,?,0,?)""",
                (str(uuid.uuid4()), audit_id, "fileX", payee_raw, amount, txn_date, time.time()),
            )


def _make_token_dict(audit_id, client_id="recCLIENT1", token="tok_abc123", expires_offset=14*86400):
    now = time.time()
    return {
        "token":      token,
        "audit_id":   audit_id,
        "client_id":  client_id,
        "created_at": int(now),
        "expires_at": int(now + expires_offset),
    }


# ── Token generation (Airtable-backed) ───────────────────────────────────────

class TestTokenGeneration:
    def test_create_token_calls_airtable(self, app, tmp_db):
        import review_app
        mock_tbl = MagicMock()
        mock_tbl.create.return_value = {"id": "recABC", "fields": {}}
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = mock_tbl
            token = review_app._create_review_token("recAUDIT1", "recCLIENT1")
        assert isinstance(token, str)
        assert len(token) >= 20
        mock_tbl.create.assert_called_once()
        call_fields = mock_tbl.create.call_args[0][0]
        assert call_fields["Audit ID"] == "recAUDIT1"
        assert call_fields["Client ID"] == "recCLIENT1"
        assert call_fields["Token"] == token

    def test_get_review_token_returns_dict(self, app, tmp_db):
        import review_app
        now = int(time.time())
        mock_tbl = MagicMock()
        mock_tbl.all.return_value = [{"id": "recABC", "fields": {
            "Token": "tok123", "Audit ID": "recAUD1", "Client ID": "recCLI1",
            "Created At": now, "Expires At": now + 14 * 86400,
        }}]
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = mock_tbl
            rec = review_app._get_review_token("tok123")
        assert rec is not None
        assert rec["token"] == "tok123"
        assert rec["audit_id"] == "recAUD1"
        assert rec["expires_at"] > now

    def test_get_review_token_missing_returns_none(self, app, tmp_db):
        import review_app
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value.all.return_value = []
            rec = review_app._get_review_token("not-a-real-token")
        assert rec is None

    def test_get_active_token_filters_expired(self, app, tmp_db):
        import review_app
        now = int(time.time())
        mock_tbl = MagicMock()
        mock_tbl.all.return_value = [{"id": "recOLD", "fields": {
            "Token": "expired_tok", "Audit ID": "recAUD1", "Client ID": "recCLI1",
            "Created At": now - 20 * 86400, "Expires At": now - 86400,
        }}]
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = mock_tbl
            result = review_app._get_active_review_token_for_audit("recAUD1")
        assert result is None

    def test_get_active_token_returns_latest(self, app, tmp_db):
        import review_app
        now = int(time.time())
        mock_tbl = MagicMock()
        mock_tbl.all.return_value = [
            {"id": "recOLD", "fields": {
                "Token": "tok_old", "Audit ID": "recAUD1", "Client ID": "recCLI1",
                "Created At": now - 7 * 86400, "Expires At": now + 7 * 86400,
            }},
            {"id": "recNEW", "fields": {
                "Token": "tok_new", "Audit ID": "recAUD1", "Client ID": "recCLI1",
                "Created At": now - 1 * 86400, "Expires At": now + 13 * 86400,
            }},
        ]
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = mock_tbl
            result = review_app._get_active_review_token_for_audit("recAUD1")
        assert result is not None
        assert result["token"] == "tok_new"


# ── review-link route ─────────────────────────────────────────────────────────

class TestReviewLinkRoute:
    def test_requires_login(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = client.post(
            f"/audits/{aid}/review-link",
            json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
        )
        assert resp.status_code == 302

    def test_generates_link(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        with patch("review_app._get_active_review_token_for_audit", return_value=None), \
             patch("review_app._create_review_token", return_value="new_token_abc"):
            resp = authed_client.post(
                f"/audits/{aid}/review-link",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "/review/new_token_abc" in data["url"]

    def test_returns_existing_active_token(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        existing = _make_token_dict(aid, token="existing_tok_xyz")
        with patch("review_app._get_active_review_token_for_audit", return_value=existing), \
             patch("review_app._create_review_token") as mock_create:
            resp = authed_client.post(
                f"/audits/{aid}/review-link",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        mock_create.assert_not_called()
        data = resp.get_json()
        assert "existing_tok_xyz" in data["url"]

    def test_404_for_unknown_audit(self, authed_client, tmp_db):
        resp = authed_client.post(
            "/audits/nonexistent_audit_id/review-link",
            json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
        )
        assert resp.status_code == 404


# ── /review/<token> page ──────────────────────────────────────────────────────

class TestClientReviewPage:
    def test_valid_token_renders_page(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok):
            resp = client.get("/review/tok_abc123")
        assert resp.status_code == 200
        assert b"Payee Review" in resp.data
        assert b"noindex" in resp.data

    def test_invalid_token_404(self, client, tmp_db):
        with patch("review_app._get_review_token", return_value=None):
            resp = client.get("/review/not-a-real-token")
        assert resp.status_code == 404

    def test_expired_token_410(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        expired = _make_token_dict(aid, expires_offset=-1)
        with patch("review_app._get_review_token", return_value=expired):
            resp = client.get("/review/some_token")
        assert resp.status_code == 410
        assert b"expired" in resp.data.lower()

    def test_no_login_required(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok):
            resp = client.get("/review/tok_abc123")
        assert resp.status_code == 200


# ── /review/<token>/data ──────────────────────────────────────────────────────

class TestClientReviewData:
    def test_returns_payees_json(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [
            ("Alpha Roofing",  50000.0, "2025-03-01"),
            ("Beta Plumbing",  30000.0, "2025-04-01"),
            ("Gamma Electric", 10000.0, "2025-05-01"),
        ])
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok), \
             patch("review_app._fetch_client_reviews_for_audit", return_value={}):
            resp = client.get("/review/tok_abc123/data")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        totals = [p["total_paid"] for p in data["payees"]]
        assert totals == sorted(totals, reverse=True)

    def test_expired_token_410(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        expired = _make_token_dict(aid, expires_offset=-1)
        with patch("review_app._get_review_token", return_value=expired):
            resp = client.get("/review/tok_abc123/data")
        assert resp.status_code == 410

    def test_invalid_token_410(self, client, tmp_db):
        with patch("review_app._get_review_token", return_value=None):
            resp = client.get("/review/fake-token/data")
        assert resp.status_code == 410

    def test_95pct_threshold_excludes_small_payees(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [
            ("Big Contractor", 90000.0, "2025-03-01"),
            ("Tiny Guy A",        500.0, "2025-04-01"),
            ("Tiny Guy B",        400.0, "2025-05-01"),
            ("Tiny Guy C",        300.0, "2025-06-01"),
            ("Tiny Guy D",        200.0, "2025-07-01"),
            ("Tiny Guy E",        100.0, "2025-08-01"),
        ])
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok), \
             patch("review_app._fetch_client_reviews_for_audit", return_value={}):
            resp = client.get("/review/tok_abc123/data")
        data = resp.get_json()
        assert data["ok"] is True
        payee_names = [p["payee_raw"] for p in data["payees"]]
        assert "Big Contractor" in payee_names
        assert data["small_count"] >= 4

    def test_includes_existing_answers(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Alpha Co", 20000.0, "2025-03-01")])
        tok = _make_token_dict(aid)
        mock_reviews = {
            "Alpha Co": {"fields": {"Answer": "Supplier", "Custom Answer": ""}},
        }
        with patch("review_app._get_review_token", return_value=tok), \
             patch("review_app._fetch_client_reviews_for_audit", return_value=mock_reviews):
            resp = client.get("/review/tok_abc123/data")
        data = resp.get_json()
        assert "Alpha Co" in data["existing_answers"]
        assert data["existing_answers"]["Alpha Co"]["answer"] == "Supplier"


# ── /review/<token>/answer ────────────────────────────────────────────────────

class TestClientReviewAnswer:
    def test_valid_answer_saves(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok), \
             patch("review_app._upsert_payee_review") as mock_upsert:
            resp = client.post(
                "/review/tok_abc123/answer",
                json={"payee_raw": "Alpha Roofing", "answer": "Subcontractor", "custom_answer": ""},
            )
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True
        mock_upsert.assert_called_once()

    def test_all_valid_answers_accepted(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        tok = _make_token_dict(aid)
        for answer in ["Subcontractor", "Supplier", "Owner or payroll", "Something else"]:
            with patch("review_app._get_review_token", return_value=tok), \
                 patch("review_app._upsert_payee_review"):
                resp = client.post(
                    "/review/tok_abc123/answer",
                    json={"payee_raw": "Test Co", "answer": answer, "custom_answer": "note"},
                )
            assert resp.status_code == 200, f"Failed for answer: {answer!r}"

    def test_invalid_answer_rejected(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok):
            resp = client.post(
                "/review/tok_abc123/answer",
                json={"payee_raw": "Alpha Roofing", "answer": "Unknown answer"},
            )
        assert resp.status_code == 400

    def test_missing_payee_raw_rejected(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok):
            resp = client.post(
                "/review/tok_abc123/answer",
                json={"answer": "Supplier"},
            )
        assert resp.status_code == 400

    def test_expired_token_410(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        expired = _make_token_dict(aid, expires_offset=-1)
        with patch("review_app._get_review_token", return_value=expired):
            resp = client.post(
                "/review/tok_abc123/answer",
                json={"payee_raw": "Test Co", "answer": "Supplier"},
            )
        assert resp.status_code == 410

    def test_no_session_required(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        tok = _make_token_dict(aid)
        with patch("review_app._get_review_token", return_value=tok), \
             patch("review_app._upsert_payee_review"):
            resp = client.post(
                "/review/tok_abc123/answer",
                json={"payee_raw": "Alpha Roofing", "answer": "Subcontractor"},
            )
        assert resp.status_code == 200


# ── sync-client-reviews route ─────────────────────────────────────────────────

class TestSyncClientReviews:
    def test_requires_login(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = client.post(
            f"/audits/{aid}/sync-client-reviews",
            json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
        )
        assert resp.status_code == 302

    def test_applies_answers_to_classifications(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Beta Plumbing", 25000.0, "2025-03-01")])
        mock_reviews = {
            "Beta Plumbing": {"fields": {"Answer": "Subcontractor", "Custom Answer": ""}},
        }
        with patch("review_app._fetch_client_reviews_for_audit", return_value=mock_reviews):
            resp = authed_client.post(
                f"/audits/{aid}/sync-client-reviews",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert data["applied"] == 1
        cls = tmp_db.get_payee_classification(aid, "Beta Plumbing")
        assert cls["payee_group"] == "likely_sub"
        assert cls["match_type"] == "client_review"
        assert cls["confirmed"] == 1

    def test_supplier_answer_maps_to_supplier_group(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Home Depot", 5000.0, "2025-03-01")])
        with patch("review_app._fetch_client_reviews_for_audit", return_value={
            "Home Depot": {"fields": {"Answer": "Supplier", "Custom Answer": ""}},
        }):
            authed_client.post(
                f"/audits/{aid}/sync-client-reviews",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        assert tmp_db.get_payee_classification(aid, "Home Depot")["payee_group"] == "supplier"

    def test_something_else_maps_to_not_sub(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("LOAN REPAYMENT", 10000.0, "2025-03-01")])
        with patch("review_app._fetch_client_reviews_for_audit", return_value={
            "LOAN REPAYMENT": {"fields": {"Answer": "Something else", "Custom Answer": "equipment loan"}},
        }):
            authed_client.post(
                f"/audits/{aid}/sync-client-reviews",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        cls = tmp_db.get_payee_classification(aid, "LOAN REPAYMENT")
        assert cls["payee_group"] == "not_sub"
        assert cls["exclude_reason"] == "equipment loan"

    def test_saves_to_library(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Gamma Electric", 15000.0, "2025-03-01")])
        with patch("review_app._fetch_client_reviews_for_audit", return_value={
            "Gamma Electric": {"fields": {"Answer": "Subcontractor", "Custom Answer": ""}},
        }):
            authed_client.post(
                f"/audits/{aid}/sync-client-reviews",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        suggestion = tmp_db.get_library_suggestion("recCLIENT1", "Gamma Electric")
        assert suggestion is not None
        assert suggestion["suggested_group"] == "likely_sub"

    def test_does_not_overwrite_staff_confirmed(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Delta Drywall", 20000.0, "2025-03-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Delta Drywall",
            vendor_id="recVENDOR1", vendor_name="Delta Drywall LLC",
            match_type="exact", payee_group="ccs_vendor", confirmed=1,
        )
        with patch("review_app._fetch_client_reviews_for_audit", return_value={
            "Delta Drywall": {"fields": {"Answer": "Supplier", "Custom Answer": ""}},
        }):
            resp = authed_client.post(
                f"/audits/{aid}/sync-client-reviews",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        assert resp.get_json()["applied"] == 0
        cls = tmp_db.get_payee_classification(aid, "Delta Drywall")
        assert cls["payee_group"] == "ccs_vendor"
        assert cls["match_type"] == "exact"

    def test_unknown_answer_skipped(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        with patch("review_app._fetch_client_reviews_for_audit", return_value={
            "Some Payee": {"fields": {"Answer": "", "Custom Answer": ""}},
        }):
            resp = authed_client.post(
                f"/audits/{aid}/sync-client-reviews",
                json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
            )
        assert resp.get_json()["applied"] == 0

    def test_404_for_unknown_audit(self, authed_client, tmp_db):
        resp = authed_client.post(
            "/audits/nonexistent/sync-client-reviews",
            json={"csrf_token": "test_csrf_32chars_paddingpadding0"},
        )
        assert resp.status_code == 404
