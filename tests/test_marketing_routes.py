"""Tests for the /marketing routes in review_app.py."""
import os
import time
from unittest.mock import MagicMock, patch

import pytest

# Must set before importing review_app
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
    """Client with a live authenticated session."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["logged_in_at"] = time.time()
        sess["csrf_token"] = "test_csrf_32chars_paddingpadding0"
    return client


class TestMarketingPageAccess:
    def test_redirects_when_not_logged_in(self, client):
        resp = client.get("/marketing")
        assert resp.status_code == 302
        assert "login" in resp.headers.get("Location", "")

    def test_accessible_when_logged_in(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.all.return_value = []
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.get("/marketing")
        assert resp.status_code == 200
        assert b"Marketing" in resp.data

    def test_shows_empty_state_when_no_ideas(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.all.return_value = []
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.get("/marketing")
        assert resp.status_code == 200
        # Empty state or the section header — either is fine
        assert b"Story Ideas" in resp.data or b"No ideas yet" in resp.data

    def test_shows_idea_when_returned(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.all.return_value = [{
                "id": "recABC123",
                "fields": {
                    "Idea": "Show how certificates on file saved audit day.",
                    "Why It's a Story": "GCs relate to audit prep stress.",
                    "Audience": "GC",
                    "Topic": "Audits",
                    "Status": "New",
                }
            }]
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.get("/marketing")
        assert resp.status_code == 200
        assert b"certificates on file" in resp.data

    def test_error_shown_on_airtable_failure(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.side_effect = Exception("Airtable down")
            resp = authed_client.get("/marketing")
        assert resp.status_code == 200
        assert b"Could not" in resp.data or b"unavailable" in resp.data.lower()

    def test_nav_contains_marketing_link(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.all.return_value = []
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.get("/marketing")
        assert b'href="/marketing"' in resp.data

    def test_note_box_present(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.all.return_value = []
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.get("/marketing")
        assert b"Quick note" in resp.data or b"quick" in resp.data.lower()


class TestMarketingApprove:
    def test_approve_requires_login(self, client):
        resp = client.post("/marketing/idea/recABC123/approve")
        assert resp.status_code == 302
        assert "login" in resp.headers.get("Location", "")

    def test_approve_redirects_to_marketing(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.update.return_value = {}
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.post("/marketing/idea/recABC123/approve")
        assert resp.status_code == 302
        assert "/marketing" in resp.headers.get("Location", "")

    def test_approve_handles_airtable_error(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.side_effect = Exception("Airtable error")
            resp = authed_client.post("/marketing/idea/recABC123/approve")
        assert resp.status_code == 302


class TestMarketingDismiss:
    def test_dismiss_requires_login(self, client):
        resp = client.post("/marketing/idea/recABC123/dismiss")
        assert resp.status_code == 302
        assert "login" in resp.headers.get("Location", "")

    def test_dismiss_redirects_to_marketing(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.update.return_value = {}
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.post("/marketing/idea/recABC123/dismiss")
        assert resp.status_code == 302
        assert "/marketing" in resp.headers.get("Location", "")


class TestMarketingNote:
    def test_note_requires_login(self, client):
        resp = client.post("/marketing/note", data={"note": "test"})
        assert resp.status_code == 302
        assert "login" in resp.headers.get("Location", "")

    def test_save_note_redirects_to_marketing(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.create.return_value = {"id": "recNEW"}
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.post("/marketing/note", data={"note": "What if we did a reel about expiring certs?"})
        assert resp.status_code == 302
        assert "/marketing" in resp.headers.get("Location", "")

    def test_empty_note_does_not_crash(self, authed_client):
        with patch("review_app._api"):
            resp = authed_client.post("/marketing/note", data={"note": ""})
        assert resp.status_code == 302
