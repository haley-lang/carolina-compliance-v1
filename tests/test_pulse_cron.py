"""test_pulse_cron.py — unit tests for pulse_cron.py health check functions."""
import os
import sys
import types
import traceback
from unittest.mock import patch, MagicMock, call

import pytest


# ---------------------------------------------------------------------------
# Minimal stubs so pulse_cron can be imported without real credentials
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _stub_env(monkeypatch):
    monkeypatch.setenv("AIRTABLE_API_KEY", "fakekey")
    monkeypatch.setenv("AIRTABLE_BASE_ID", "appFAKEBASEID1234")
    monkeypatch.setenv("PULSE_WEB_URL", "")       # disable HTTP transport
    monkeypatch.setenv("PULSE_SECRET_TOKEN", "")


@pytest.fixture
def pc():
    """Import (or reload) pulse_cron so env stubs are active."""
    import importlib
    import pulse_cron
    importlib.reload(pulse_cron)
    return pulse_cron


# ---------------------------------------------------------------------------
# 1. check_airtable_space — all table fetches fail → "could not count"
# ---------------------------------------------------------------------------

def test_airtable_space_all_fail_reports_could_not_count(pc):
    mock_tbl = MagicMock()
    mock_tbl.all.side_effect = Exception("403 Forbidden")
    mock_api = MagicMock()
    mock_api.return_value.table.return_value = mock_tbl

    pc._ev_buf.clear()
    with patch.object(pc, "_api", mock_api):
        result = pc.check_airtable_space()

    assert result["total_records"] is None
    assert result["level"] == "error"
    # Event title must say "could not count"
    titles = [e["title"] for e in pc._ev_buf]
    assert any("could not count" in t for t in titles), f"titles: {titles}"


def test_airtable_space_all_fail_does_not_say_ok(pc):
    """Regression: 0 records with level 'ok' must never happen when all fetches fail."""
    mock_tbl = MagicMock()
    mock_tbl.all.side_effect = RuntimeError("network error")
    mock_api = MagicMock()
    mock_api.return_value.table.return_value = mock_tbl

    pc._ev_buf.clear()
    with patch.object(pc, "_api", mock_api):
        result = pc.check_airtable_space()

    assert not (result["total_records"] == 0 and result["level"] == "ok"), (
        "Must not silently report 0 records with 'ok' when all fetches failed"
    )


def test_airtable_space_counts_correctly_on_success(pc):
    """When tables return records, total is the sum of all counts."""
    def _make_tbl(n):
        tbl = MagicMock()
        tbl.all.return_value = [{"id": f"rec{i}", "fields": {}} for i in range(n)]
        return tbl

    # 10 tables, each returning 100 records
    mock_api_inst = MagicMock()
    mock_api_inst.table.return_value = _make_tbl(100)
    mock_api = MagicMock(return_value=mock_api_inst)

    pc._ev_buf.clear()
    with patch.object(pc, "_api", mock_api):
        result = pc.check_airtable_space()

    assert result["total_records"] == 1000
    assert result["level"] == "ok"


def test_airtable_space_partial_success_uses_succeeded_tables(pc):
    """If some tables fail and some succeed, total reflects only the succeeded ones."""
    call_count = {"n": 0}

    def _side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 5:
            raise Exception("fail")
        return [{"id": "rec1", "fields": {}}]

    mock_tbl = MagicMock()
    mock_tbl.all.side_effect = _side_effect
    mock_api_inst = MagicMock()
    mock_api_inst.table.return_value = mock_tbl
    mock_api = MagicMock(return_value=mock_api_inst)

    pc._ev_buf.clear()
    with patch.object(pc, "_api", mock_api):
        result = pc.check_airtable_space()

    assert result["total_records"] == 5   # 5 tables succeeded with 1 record each
    assert result["level"] == "ok"


# ---------------------------------------------------------------------------
# 2. check_pipeline_stages — no-activity vs broken
# ---------------------------------------------------------------------------

def test_pipeline_empty_table_is_no_activity_not_error(pc):
    """Empty Incoming Extractions table → activity='none', level='ok'."""
    mock_tbl = MagicMock()
    mock_tbl.all.return_value = []
    with patch.object(pc, "_table", return_value=mock_tbl):
        pc._ev_buf.clear()
        result = pc.check_pipeline_stages()

    assert result["level"] == "ok"
    assert result["activity"] == "none"
    assert all(v == 0 for v in result["by_stage"].values())
    titles = [e["title"] for e in pc._ev_buf]
    assert any("no activity" in t.lower() for t in titles), f"titles: {titles}"


def test_pipeline_exception_gives_error_not_no_activity(pc):
    """API exception → activity='error', level='error'."""
    mock_tbl = MagicMock()
    mock_tbl.all.side_effect = Exception("422 Unprocessable Entity")
    with patch.object(pc, "_table", return_value=mock_tbl):
        pc._ev_buf.clear()
        result = pc.check_pipeline_stages()

    assert result["level"] == "error"
    assert result["activity"] == "error"


def test_pipeline_counts_known_statuses(pc):
    """Records with known Processing Status values are counted in correct buckets."""
    def _rec(status, created="2026-11-11T12:00:00.000Z"):  # 1 day old, well under thresholds
        return {
            "id": "recXXX",
            "createdTime": created,
            "fields": {"Processing Status": status, "Review Status": "", "Source Filename": "f.pdf"},
        }

    recs = [
        _rec("New"),
        _rec("Processed"),
        _rec("Processed"),
        _rec("Needs Review"),
        _rec("Extracted"),
    ]
    mock_tbl = MagicMock()
    mock_tbl.all.return_value = recs

    with patch.object(pc, "_table", return_value=mock_tbl):
        pc._ev_buf.clear()
        result = pc.check_pipeline_stages()

    assert result["level"] == "ok"
    assert result["activity"] == "active"
    assert result["by_stage"]["received"] == 1    # "New"
    assert result["by_stage"]["confirmed"] == 2   # "Processed" ×2
    assert result["by_stage"]["pending_review"] == 1  # "Needs Review"
    assert result["by_stage"]["extracted"] == 1   # "Extracted"


def test_pipeline_does_not_sort_by_created(pc):
    """tbl.all() must be called without sort=['Created'] to avoid 422."""
    mock_tbl = MagicMock()
    mock_tbl.all.return_value = []
    with patch.object(pc, "_table", return_value=mock_tbl):
        pc.check_pipeline_stages()

    call_kwargs = mock_tbl.all.call_args
    # sort parameter must not contain "Created"
    sort_arg = (call_kwargs.kwargs or {}).get("sort") or (call_kwargs.args[1] if len(call_kwargs.args) > 1 else None)
    assert sort_arg is None or "Created" not in str(sort_arg)


# ---------------------------------------------------------------------------
# 3. main() exit code — problems should not cause sys.exit(1)
# ---------------------------------------------------------------------------

def test_main_does_not_exit_nonzero_on_error_level(pc):
    """main() must exit 0 even when checks find errors."""
    error_space    = {"total_records": None, "level": "error", "limit": 50_000,
                      "tables": [], "fastest_growing": None}
    error_pipeline = {"by_stage": {"received": 0, "extracted": 0,
                                   "pending_review": 0, "confirmed": 0},
                      "oldest_pending_days": None, "avg_days_received_to_confirmed": None,
                      "stuck": [], "level": "error", "activity": "error"}
    ok_email  = {"last_check": None, "last_received": None, "today_received": 0,
                 "today_processed": 0, "today_skipped": 0, "skipped_items": [], "level": "ok"}
    ok_wr     = {"writes_per_hour": [], "level": "ok"}
    ok_svcs   = [{"name": "Airtable API", "status": "Reachable", "last_ok": None,
                  "level": "ok", "detail": ""}]

    with patch.object(pc, "check_airtable_space",   return_value=error_space), \
         patch.object(pc, "check_email_intake",      return_value=ok_email), \
         patch.object(pc, "check_pipeline_stages",   return_value=error_pipeline), \
         patch.object(pc, "check_write_rate",        return_value=ok_wr), \
         patch.object(pc, "check_outside_services",  return_value=ok_svcs), \
         patch.object(pc, "_send_daily_summary"), \
         patch.object(pc, "_send_to_web_service"), \
         patch.object(pc, "_fetch_health_data",     return_value={}):
        # Should not raise SystemExit
        try:
            pc.main()
        except SystemExit as e:
            pytest.fail(f"main() called sys.exit({e.code}) — should exit 0 on problem level")


# ---------------------------------------------------------------------------
# 4. _send_to_web_service — log line confirms HTTP status on success
# ---------------------------------------------------------------------------

def test_send_to_web_service_logs_http_status_on_success(pc, capsys):
    """Successful POST must print a line containing the HTTP status code."""
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b'{"ok": true}'
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch.dict(os.environ, {"PULSE_WEB_URL": "https://fake.example.com",
                                  "PULSE_SECRET_TOKEN": "tok"}):
        with patch("pulse_cron._ur.urlopen", return_value=mock_resp):
            pc._send_to_web_service(heartbeats=["pulse_cron"], events=[], prune=False)

    captured = capsys.readouterr()
    assert "200" in captured.out, f"HTTP status not logged: {captured.out!r}"
    assert "Check-in OK" in captured.out or "check-in" in captured.out.lower()


def test_send_to_web_service_falls_back_on_failure(pc, capsys):
    """Network failure must fall back to local health_store without raising."""
    with patch.dict(os.environ, {"PULSE_WEB_URL": "https://fake.example.com",
                                  "PULSE_SECRET_TOKEN": "tok"}):
        with patch("pulse_cron._ur.urlopen", side_effect=OSError("connection refused")):
            with patch.object(pc.health_store, "record_heartbeat") as mock_hb:
                pc._send_to_web_service(heartbeats=["pulse_cron"], events=[], prune=False)

    mock_hb.assert_called_once_with("pulse_cron")
    captured = capsys.readouterr()
    assert "falling back" in captured.out.lower()
