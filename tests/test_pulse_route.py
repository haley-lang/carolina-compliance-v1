"""test_pulse_route.py — /pulse renders 200 even with no data."""
import pytest
from unittest.mock import patch, MagicMock


def test_pulse_returns_200_empty_health_store(client):
    """Pulse page must not 500 when health store is empty."""
    with patch("review_app._hs") as mock_hs, \
         patch("review_app._api") as mock_api:
        mock_hs.get_recent_events.return_value = []
        mock_hs.get_last_heartbeat.return_value = None
        mock_hs.get_write_rate.return_value = 0
        tbl_mock = MagicMock()
        tbl_mock.all.return_value = []
        tbl_mock.first.return_value = None
        mock_api.return_value.table.return_value = tbl_mock
        resp = client.get("/pulse")
    assert resp.status_code == 200


def test_pulse_returns_200_airtable_errors(client):
    """Pulse page must not 500 when every Airtable call raises."""
    with patch("review_app._hs") as mock_hs, \
         patch("review_app._api") as mock_api:
        mock_hs.get_recent_events.return_value = []
        mock_hs.get_last_heartbeat.return_value = None
        mock_hs.get_write_rate.return_value = 0
        tbl_mock = MagicMock()
        tbl_mock.all.side_effect = Exception("Airtable down")
        mock_api.return_value.table.return_value = tbl_mock
        resp = client.get("/pulse")
    assert resp.status_code == 200
