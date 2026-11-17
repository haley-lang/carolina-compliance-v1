"""Tests for the payee sorting-sheet export and import routes."""
import io
import os
import time
import tempfile
import uuid
from unittest.mock import MagicMock, patch

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


def _make_audit(tmp_db):
    rec = tmp_db.create_audit(
        client_id="recCLIENT1",
        client_name="Test GC",
        coverage="GL",
        start_date="2025-01-01",
        end_date="2025-12-31",
    )
    return rec["id"]


def _insert_payments(tmp_db, audit_id, rows):
    """Insert rows = [(payee_raw, amount, txn_date), ...]"""
    import sqlite3
    db_path = os.environ["AUDIT_DB_PATH"]
    with sqlite3.connect(db_path) as conn:
        for payee_raw, amount, txn_date in rows:
            conn.execute(
                """INSERT INTO payments (id, audit_id, file_id, payee_raw, amount,
                   txn_date, excluded, created_at)
                   VALUES (?,?,?,?,?,?,0,?)""",
                (str(uuid.uuid4()), audit_id, "fileX", payee_raw, amount, txn_date, time.time()),
            )


# ── Export route ──────────────────────────────────────────────────────────────

class TestSortingSheetExport:
    def test_requires_login(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = client.get(f"/audits/{aid}/payees/sorting-sheet")
        assert resp.status_code == 302
        assert "login" in resp.headers.get("Location", "")

    def test_returns_xlsx(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [
            ("Acme Roofing", 15000.0, "2025-03-01"),
            ("Home Depot",    2500.0, "2025-04-01"),
            ("Small Guy",      300.0, "2025-06-01"),
        ])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Acme Roofing",
            payee_group="likely_sub", confirmed=0,
        )
        resp = authed_client.get(f"/audits/{aid}/payees/sorting-sheet")
        assert resp.status_code == 200
        assert "spreadsheet" in resp.content_type

    def test_xlsx_has_correct_columns(self, authed_client, tmp_db):
        import openpyxl
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Alpha LLC", 10000.0, "2025-02-01")])
        resp = authed_client.get(f"/audits/{aid}/payees/sorting-sheet")
        wb = openpyxl.load_workbook(io.BytesIO(resp.data))
        ws = wb.active
        headers = [ws.cell(row=1, column=c).value for c in range(1, 9)]
        assert headers[0] == "Payee Name"
        assert headers[1] == "Total Paid"
        assert headers[4] == "System Guess"
        assert headers[5] == "Billy's Answer"
        assert headers[6] == "Trade"

    def test_rows_sorted_by_total_paid_desc(self, authed_client, tmp_db):
        import openpyxl
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [
            ("Low Payee",  1000.0, "2025-01-01"),
            ("High Payee", 9000.0, "2025-01-01"),
            ("Mid Payee",  4000.0, "2025-01-01"),
        ])
        resp = authed_client.get(f"/audits/{aid}/payees/sorting-sheet")
        wb = openpyxl.load_workbook(io.BytesIO(resp.data))
        ws = wb.active
        names = [ws.cell(row=r, column=1).value for r in range(2, 5)]
        assert names[0] == "High Payee"
        assert names[1] == "Mid Payee"
        assert names[2] == "Low Payee"

    def test_small_payments_separator(self, authed_client, tmp_db):
        import openpyxl
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [
            ("Big Co",   5000.0, "2025-01-01"),
            ("Tiny Guy",  200.0, "2025-01-01"),
        ])
        resp = authed_client.get(f"/audits/{aid}/payees/sorting-sheet")
        wb = openpyxl.load_workbook(io.BytesIO(resp.data))
        ws = wb.active
        all_values = [ws.cell(row=r, column=1).value for r in range(2, ws.max_row + 1)]
        separator_rows = [v for v in all_values if v and "small payments" in str(v).lower()]
        assert len(separator_rows) == 1

    def test_possible_match_shown_as_needs_confirming(self, authed_client, tmp_db):
        import openpyxl
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Acme Corp", 5000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Acme Corp",
            payee_group="unclear", match_type="possible",
            vendor_id="recVENDOR1", vendor_name="Acme Construction",
            confirmed=0,
        )
        resp = authed_client.get(f"/audits/{aid}/payees/sorting-sheet")
        wb = openpyxl.load_workbook(io.BytesIO(resp.data))
        ws = wb.active
        guess = ws.cell(row=2, column=5).value
        assert guess is not None and "confirming" in guess.lower()
        assert "Acme Construction" in guess

    def test_ccs_vendor_shown_as_sub(self, authed_client, tmp_db):
        import openpyxl
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("TruTeam Inc", 8000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="TruTeam Inc",
            payee_group="ccs_vendor", match_type="exact",
            vendor_id="recV1", vendor_name="TruTeam", confirmed=1,
        )
        resp = authed_client.get(f"/audits/{aid}/payees/sorting-sheet")
        wb = openpyxl.load_workbook(io.BytesIO(resp.data))
        ws = wb.active
        guess = ws.cell(row=2, column=5).value
        assert guess == "Sub"

    def test_running_pct_increases(self, authed_client, tmp_db):
        import openpyxl
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [
            ("Alpha", 6000.0, "2025-01-01"),
            ("Beta",  4000.0, "2025-01-01"),
        ])
        resp = authed_client.get(f"/audits/{aid}/payees/sorting-sheet")
        wb = openpyxl.load_workbook(io.BytesIO(resp.data))
        ws = wb.active
        run1 = ws.cell(row=2, column=4).value  # 60%
        run2 = ws.cell(row=3, column=4).value  # 100%
        assert run1 is not None and run2 is not None
        assert float(run2) > float(run1)
        assert abs(float(run2) - 1.0) < 0.001


# ── Import route ──────────────────────────────────────────────────────────────

class TestSortingSheetImport:
    def _make_xlsx(self, rows):
        """rows = [(payee_name, billy_answer, trade), ...]"""
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Payee Name", "Total Paid", "% of Total", "Running %",
                   "System Guess", "Billy's Answer", "Trade", "Notes"])
        for payee, answer, trade in rows:
            ws.append([payee, 1000.0, 0.1, 0.1, "", answer, trade, ""])
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return buf

    def test_requires_login(self, client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = client.post(f"/audits/{aid}/payees/import-sorting-sheet")
        assert resp.status_code == 302

    def test_no_file_returns_400(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0"},
        )
        assert resp.status_code == 400

    def test_supplier_answer_sets_supplier_group(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Grainger Supply", 3000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Grainger Supply",
            payee_group="unclear", confirmed=0,
        )
        xlsx = self._make_xlsx([("Grainger Supply", "Supplier", "")])
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert data["updated"] == 1
        cls = tmp_db.get_payee_classification(aid, "Grainger Supply")
        assert cls["payee_group"] == "supplier"
        assert cls["confirmed"] == 1

    def test_not_a_sub_answer(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("ADP Payroll", 5000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="ADP Payroll",
            payee_group="unclear", confirmed=0,
        )
        xlsx = self._make_xlsx([("ADP Payroll", "Not a sub", "")])
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        assert resp.get_json()["ok"] is True
        cls = tmp_db.get_payee_classification(aid, "ADP Payroll")
        assert cls["payee_group"] == "not_sub"

    def test_sub_with_no_vendor_becomes_likely_sub(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Bob's Drywall", 2000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Bob's Drywall",
            payee_group="unclear", confirmed=0,
        )
        xlsx = self._make_xlsx([("Bob's Drywall", "Sub", "")])
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        assert resp.get_json()["ok"] is True
        cls = tmp_db.get_payee_classification(aid, "Bob's Drywall")
        assert cls["payee_group"] == "likely_sub"

    def test_sub_with_possible_match_stays_likely_sub(self, authed_client, tmp_db):
        """Billy's Sub answer on a possible/likely match → likely_sub, not ccs_vendor.
        Only an exact system match (Haley must confirm) promotes to ccs_vendor."""
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Acme Roofing", 7000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Acme Roofing",
            payee_group="unclear", match_type="possible",
            vendor_id="recV1", vendor_name="Acme Construction LLC",
            confirmed=0,
        )
        xlsx = self._make_xlsx([("Acme Roofing", "Sub", "")])
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        assert resp.get_json()["ok"] is True
        cls = tmp_db.get_payee_classification(aid, "Acme Roofing")
        assert cls["payee_group"] == "likely_sub"
        assert cls["vendor_id"] == "recV1"  # vendor suggestion retained

    def test_sub_with_exact_match_becomes_ccs_vendor(self, authed_client, tmp_db):
        """Billy's Sub answer on an exact system match → ccs_vendor."""
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Acme Roofing LLC", 7000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Acme Roofing LLC",
            payee_group="ccs_vendor", match_type="exact",
            vendor_id="recV1", vendor_name="Acme Roofing LLC",
            confirmed=0,
        )
        xlsx = self._make_xlsx([("Acme Roofing LLC", "Sub", "")])
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        assert resp.get_json()["ok"] is True
        cls = tmp_db.get_payee_classification(aid, "Acme Roofing LLC")
        assert cls["payee_group"] == "ccs_vendor"
        assert cls["vendor_id"] == "recV1"

    def test_blank_answer_skipped(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Unknown Co", 1000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Unknown Co",
            payee_group="unclear", confirmed=0,
        )
        xlsx = self._make_xlsx([("Unknown Co", "", "")])
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        data = resp.get_json()
        assert data["ok"] is True
        assert data["updated"] == 0
        assert data["skipped"] >= 1
        # Group must NOT have changed
        cls = tmp_db.get_payee_classification(aid, "Unknown Co")
        assert cls["payee_group"] == "unclear"
        assert cls["confirmed"] == 0

    def test_saves_to_library(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Silva Painting", 4000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Silva Painting",
            payee_group="unclear", confirmed=0,
        )
        xlsx = self._make_xlsx([("Silva Painting", "Sub", "Painting")])
        authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        lib = tmp_db.get_library_suggestion("recCLIENT1", "Silva Painting")
        assert lib is not None
        assert lib["suggested_group"] == "likely_sub"

    def test_trade_updated_on_import(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Martinez Electric", 6000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Martinez Electric",
            payee_group="unclear", confirmed=0,
        )
        xlsx = self._make_xlsx([("Martinez Electric", "Sub", "Electrical")])
        authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (xlsx, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        cls = tmp_db.get_payee_classification(aid, "Martinez Electric")
        assert cls["trade"] == "Electrical"

    def test_separator_rows_skipped(self, authed_client, tmp_db):
        aid = _make_audit(tmp_db)
        _insert_payments(tmp_db, aid, [("Real Payee", 2000.0, "2025-01-01")])
        tmp_db.upsert_payee_classification(
            audit_id=aid, client_id="recCLIENT1", payee_raw="Real Payee",
            payee_group="unclear", confirmed=0,
        )
        # Include a separator row and a TOTAL row in the import file
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Payee Name", "Total Paid", "% of Total", "Running %",
                   "System Guess", "Billy's Answer", "Trade", "Notes"])
        ws.append(["Real Payee", 2000, 0.5, 0.5, "", "Sub", "", ""])
        ws.append(["Small payments (under $1,000)  —  2 payees", "", "", "", "", "", "", ""])
        ws.append(["TOTAL", 2000, "", "", "", "", "", ""])
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        resp = authed_client.post(
            f"/audits/{aid}/payees/import-sorting-sheet",
            data={"csrf_token": "test_csrf_32chars_paddingpadding0",
                  "file": (buf, "sort.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            content_type="multipart/form-data",
        )
        data = resp.get_json()
        assert data["ok"] is True
        assert data["updated"] == 1
