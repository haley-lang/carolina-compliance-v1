"""Tests for the /follow-ups route and five-tab nav completeness."""
import os
import time
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("REVIEW_PASSWORD", "test_review_pass")


@pytest.fixture
def app():
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


def _mock_api_for_follow_ups(no_vendors=False):
    """Return a mock _api() that satisfies follow-ups' Airtable calls."""
    mock_api = MagicMock()
    mock_clients_tbl = MagicMock()
    mock_clients_tbl.all.return_value = [
        {"id": "recCLIENT1", "fields": {"Client Name": "RTR Homes"}},
    ]
    mock_gaps_tbl = MagicMock()
    mock_gaps_tbl.all.return_value = []
    mock_vendors_tbl = MagicMock()
    if no_vendors:
        mock_vendors_tbl.all.return_value = []
    else:
        mock_vendors_tbl.all.return_value = [
            {"id": "recV1", "fields": {
                "Vendor Name": "Smith Plumbing",
                "Vendor Client Assignments": ["recVCA1"],
            }},
        ]
    mock_assignments_tbl = MagicMock()
    mock_assignments_tbl.all.return_value = [
        {"id": "recVCA1", "fields": {
            "Vendor": ["recV1"],
            "Client": ["recCLIENT1"],
        }},
    ]
    mock_policies_tbl = MagicMock()
    mock_policies_tbl.all.return_value = []

    def table_side_effect(base_id, table_id):
        from review_app import (
            CLIENTS_TABLE_ID, GAP_REQUESTS_TABLE_ID,
            VENDORS_TABLE_ID,
        )
        from airtable_constants import (
            VENDOR_CLIENT_ASSIGNMENTS_TABLE as VCA_TABLE,
            INSURANCE_POLICIES_TABLE as POL_TABLE,
        )
        if table_id == CLIENTS_TABLE_ID:
            return mock_clients_tbl
        if table_id == GAP_REQUESTS_TABLE_ID:
            return mock_gaps_tbl
        if table_id == VENDORS_TABLE_ID:
            return mock_vendors_tbl
        if table_id == VCA_TABLE:
            return mock_assignments_tbl
        return mock_policies_tbl

    mock_api.return_value.table.side_effect = table_side_effect
    return mock_api


class TestFollowUpsAccess:
    def test_redirects_when_not_logged_in(self, client):
        resp = client.get("/follow-ups")
        assert resp.status_code == 302
        assert "login" in resp.headers.get("Location", "")

    def test_accessible_when_logged_in(self, authed_client):
        with patch("review_app._api") as mock_api, \
             patch("review_app.audit_store", create=True) as mock_store:
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/follow-ups")
        assert resp.status_code == 200

    def test_shows_follow_ups_as_active_nav_tab(self, authed_client):
        with patch("review_app._api") as mock_api, \
             patch("review_app.audit_store", create=True):
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/follow-ups")
        assert b'nav-link--active' in resp.data
        # The active tab text should be Follow-ups
        body = resp.data.decode()
        active_idx = body.find("nav-link--active")
        assert "Follow-ups" in body[active_idx:active_idx + 100]

    def test_nav_has_all_five_tabs(self, authed_client):
        with patch("review_app._api") as mock_api, \
             patch("review_app.audit_store", create=True):
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/follow-ups")
        assert b'href="/"' in resp.data
        assert b'href="/queue"' in resp.data
        assert b'href="/follow-ups"' in resp.data
        assert b'href="/audits"' in resp.data
        assert b'href="/marketing"' in resp.data

    def test_client_id_param_accepted(self, authed_client):
        with patch("review_app._api") as mock_api, \
             patch("review_app.audit_store", create=True):
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/follow-ups?client_id=recABC123")
        assert resp.status_code == 200


class TestFollowUpsFiltering:
    def test_stage_filter_param_accepted(self, authed_client):
        with patch("review_app._api") as mock_api, \
             patch("review_app.audit_store", create=True):
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/follow-ups?stage=sent")
        assert resp.status_code == 200

    def test_window_params_accepted(self, authed_client):
        with patch("review_app._api") as mock_api, \
             patch("review_app.audit_store", create=True):
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/follow-ups?window_start=2026-01-01&window_end=2026-12-31")
        assert resp.status_code == 200


class TestNavConsistency:
    """Verify the five-tab nav is consistent across all main pages."""

    _FIVE_TAB_HREFS = [b'href="/"', b'href="/queue"', b'href="/follow-ups"',
                       b'href="/audits"', b'href="/marketing"']

    def _assert_five_tabs(self, resp):
        for href in self._FIVE_TAB_HREFS:
            assert href in resp.data, f"Missing tab {href} in nav"

    def test_queue_has_five_tabs(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/queue")
        self._assert_five_tabs(resp)

    def test_audits_has_five_tabs(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/audits")
        self._assert_five_tabs(resp)

    def test_marketing_has_five_tabs(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/marketing")
        self._assert_five_tabs(resp)

    def test_corrections_has_five_tabs(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/corrections")
        self._assert_five_tabs(resp)

    def test_pulse_has_five_tabs(self, authed_client):
        with patch("review_app._api") as mock_api, \
             patch("review_app.health_store", create=True) as mock_hs:
            mock_hs.get_all_heartbeats.return_value = []
            mock_hs.get_recent_events.return_value = []
            mock_api.return_value.table.return_value = MagicMock(all=MagicMock(return_value=[]))
            resp = authed_client.get("/pulse")
        self._assert_five_tabs(resp)
