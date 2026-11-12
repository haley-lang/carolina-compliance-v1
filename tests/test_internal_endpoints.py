"""test_internal_endpoints.py — /internal/pulse-checkin and /internal/health-data."""
import os
import pytest
from unittest.mock import patch, MagicMock


VALID_TOKEN = "test-secret-token-32chars-padded00"


@pytest.fixture
def app_with_token(app):
    """App with PULSE_SECRET_TOKEN set."""
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        yield app


# ── /internal/pulse-checkin ──────────────────────────────────────────────────

def test_pulse_checkin_missing_token_returns_401(app):
    """Request without Authorization header must be rejected."""
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        c = app.test_client()
        resp = c.post("/internal/pulse-checkin", json={})
    assert resp.status_code == 401


def test_pulse_checkin_wrong_token_returns_401(app):
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        c = app.test_client()
        resp = c.post(
            "/internal/pulse-checkin",
            json={},
            headers={"Authorization": "Bearer wrong-token"},
        )
    assert resp.status_code == 401


def test_pulse_checkin_stores_heartbeat(app):
    """Valid POST with heartbeats calls record_heartbeat for each service."""
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        with patch("review_app._hs") as mock_hs:
            c = app.test_client()
            resp = c.post(
                "/internal/pulse-checkin",
                json={"heartbeats": ["pulse_cron"], "events": [], "prune": False},
                headers={"Authorization": f"Bearer {VALID_TOKEN}"},
            )
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}
    mock_hs.record_heartbeat.assert_called_once_with("pulse_cron")


def test_pulse_checkin_stores_events(app):
    """Valid POST with events calls record_event for each one."""
    events = [
        {"category": "pipeline", "level": "ok", "title": "test", "body": "", "table_name": ""},
        {"category": "email_intake", "level": "warn", "title": "skipped", "body": "x", "table_name": "Incoming Documents"},
    ]
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        with patch("review_app._hs") as mock_hs:
            c = app.test_client()
            resp = c.post(
                "/internal/pulse-checkin",
                json={"heartbeats": [], "events": events, "prune": False},
                headers={"Authorization": f"Bearer {VALID_TOKEN}"},
            )
    assert resp.status_code == 200
    assert mock_hs.record_event.call_count == 2


def test_pulse_checkin_prune_flag(app):
    """prune=True triggers prune_old_data."""
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        with patch("review_app._hs") as mock_hs:
            c = app.test_client()
            resp = c.post(
                "/internal/pulse-checkin",
                json={"heartbeats": [], "events": [], "prune": True},
                headers={"Authorization": f"Bearer {VALID_TOKEN}"},
            )
    assert resp.status_code == 200
    mock_hs.prune_old_data.assert_called_once_with(keep_days=7)


# ── /internal/health-data ────────────────────────────────────────────────────

def test_health_data_missing_token_returns_401(app):
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        c = app.test_client()
        resp = c.get("/internal/health-data")
    assert resp.status_code == 401


def test_health_data_returns_expected_shape(app):
    """Valid GET returns heartbeats, write_rates, and recent_events keys."""
    with patch.dict(os.environ, {"PULSE_SECRET_TOKEN": VALID_TOKEN}):
        with patch("review_app._hs") as mock_hs:
            mock_hs.get_all_heartbeats.return_value = {"pulse_cron": 1234567890.0}
            mock_hs.get_write_rate.return_value = 5
            mock_hs.get_recent_events.return_value = []
            c = app.test_client()
            resp = c.get(
                "/internal/health-data",
                headers={"Authorization": f"Bearer {VALID_TOKEN}"},
            )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "heartbeats" in data
    assert "write_rates" in data
    assert "recent_events" in data
    assert data["heartbeats"] == {"pulse_cron": 1234567890.0}
    assert isinstance(data["write_rates"], dict)
    assert data["recent_events"] == []
