"""Tests for review_app.py — login, flags, approve, corrections, and safety invariants."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Set env var before importing review_app
os.environ.setdefault("REVIEW_PASSWORD", "test_review_pass")


@pytest.fixture
def app():
    import review_app
    review_app.app.config["TESTING"] = True
    review_app.app.config["WTF_CSRF_ENABLED"] = False
    return review_app.app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def authed_client(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
    return client


# ── Login ─────────────────────────────────────────────────────────────────────

def test_unauthed_queue_redirects_to_login(client):
    resp = client.get("/queue", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_unauthed_detail_redirects_to_login(client):
    resp = client.get("/detail/recABC", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_unauthed_action_redirects_to_login(client):
    resp = client.post("/action/recABC", data={"action": "approve"}, follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_wrong_password_rejected(client):
    resp = client.post("/login", data={"password": "wrong"}, follow_redirects=True)
    assert b"Wrong password" in resp.data


def test_correct_password_grants_access(client):
    resp = client.post("/login", data={"password": "test_review_pass"}, follow_redirects=False)
    assert resp.status_code == 302
    assert "/queue" in resp.headers["Location"]


# ── compute_flags ─────────────────────────────────────────────────────────────

from review_app import compute_flags

_GL = {
    "policy_type": "Commercial General Liability",
    "policy_number": "GL-001",
    "carrier": "Hartford",
    "effective_date": "2025-01-01",
    "expiration_date": "2026-01-01",
    "coverage_limits": "$1M",
    "policy_basis": "occurrence",
    "additional_insured_checked": False,
    "waiver_of_subrogation_checked": False,
    "primary_noncontributory_checked": False,
}


def test_flag_ai_disagreed_when_field_set():
    f = {"AI Disagreements": "GL.wos: False vs True", "Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme", "policies": [_GL]}
    assert compute_flags(f, raw)["ai_disagreed"] is True


def test_flag_ai_disagreed_false_when_empty():
    f = {"AI Disagreements": "", "Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme", "policies": [_GL]}
    assert compute_flags(f, raw)["ai_disagreed"] is False


def test_flag_missing_fields_when_no_policy_number():
    pol = {**_GL, "policy_number": ""}
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme", "policies": [pol]}
    assert compute_flags(f, raw)["missing_fields"] is True


def test_flag_missing_fields_when_no_dates():
    pol = {**_GL, "effective_date": "", "expiration_date": ""}
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme", "policies": [pol]}
    assert compute_flags(f, raw)["missing_fields"] is True


def test_flag_missing_fields_false_when_complete():
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme", "policies": [_GL]}
    assert compute_flags(f, raw)["missing_fields"] is False


def test_flag_low_confidence():
    f = {"Confidence Score": 0.80}
    raw = {"document_type": "COI", "policies": []}
    assert compute_flags(f, raw)["low_confidence"] is True


def test_flag_low_confidence_false_at_threshold():
    f = {"Confidence Score": 0.85}
    raw = {"document_type": "COI", "policies": []}
    assert compute_flags(f, raw)["low_confidence"] is False


def test_flag_non_coi():
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "cancellation_notice", "policies": []}
    assert compute_flags(f, raw)["non_coi"] is True


def test_flag_non_coi_false_for_coi():
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "policies": []}
    assert compute_flags(f, raw)["non_coi"] is False


def test_flag_changed_vs_prior_checkbox():
    prior = {"policies": [{**_GL, "waiver_of_subrogation_checked": False}]}
    current = {"document_type": "COI", "policies": [{**_GL, "waiver_of_subrogation_checked": True}]}
    f = {"Confidence Score": 0.99, "Prior Raw JSON (superseded)": json.dumps(prior)}
    assert compute_flags(f, current)["changed_vs_prior"] is True


def test_flag_changed_vs_prior_false_when_same():
    prior = {"policies": [_GL]}
    current = {"document_type": "COI", "policies": [_GL]}
    f = {"Confidence Score": 0.99, "Prior Raw JSON (superseded)": json.dumps(prior)}
    assert compute_flags(f, current)["changed_vs_prior"] is False


def test_flag_multi_company_with_and():
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme LLC and Beta Inc", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is True


def test_flag_multi_company_false_single():
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme LLC", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is False


def test_flag_needs_look_true_if_any_flag():
    f = {"AI Disagreements": "GL.wos: F vs T", "Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "X", "policies": []}
    assert compute_flags(f, raw)["needs_look"] is True


# ── Approve: corrections written, status updated ──────────────────────────────

def _make_rec(named_insured="RTR LLC", policy_number="GL-001", wos=False):
    raw = {
        "document_type": "COI",
        "named_insured": named_insured,
        "certificate_holder": "GC Inc",
        "contact_emails": [],
        "policies": [{**_GL, "policy_number": policy_number, "waiver_of_subrogation_checked": wos}],
        "confidence": 0.98,
    }
    return {
        "id": "recTEST",
        "fields": {
            "Source Filename": "cert01.json",
            "Named Insured": named_insured,
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.98,
        },
    }


@pytest.fixture
def mock_ie_table():
    return MagicMock()


@pytest.fixture
def mock_corr_table():
    return MagicMock()


def test_approve_no_edits_sets_approved(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recTEST", data={
            "action": "approve",
            "why": "",
            "named_insured": "RTR LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "Commercial General Liability",
            "policy_0_policy_number": "GL-001",
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        })

    update_args = mock_ie_table.update.call_args
    fields_written = update_args[0][1]
    assert fields_written["Review Status"] == "Approved"
    assert "Processing Status" not in fields_written
    mock_corr_table.create.assert_not_called()


def test_approve_with_edit_sets_approved_edited(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec(policy_number="GL-001")
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recTEST", data={
            "action": "approve",
            "why": "typo",
            "named_insured": "RTR LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "Commercial General Liability",
            "policy_0_policy_number": "GL-002",   # changed
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        })

    fields_written = mock_ie_table.update.call_args[0][1]
    assert fields_written["Review Status"] == "Approved + Edited"
    assert "Processing Status" not in fields_written


def test_approve_each_changed_field_writes_one_correction(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec(policy_number="GL-001")
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data={
            "action": "approve",
            "why": "fixing",
            "named_insured": "RTR LLC (edited)",   # changed
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "Commercial General Liability",
            "policy_0_policy_number": "GL-002",   # changed
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        })

    # Two fields changed → two Corrections rows
    assert mock_corr_table.create.call_count == 2
    # Each row should have Learning Status = "New"
    for call in mock_corr_table.create.call_args_list:
        assert call[0][0]["Learning Status"] == "New"


def test_approve_correction_row_has_required_fields(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec(policy_number="GL-OLD")
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data={
            "action": "approve",
            "why": "the reason",
            "named_insured": "RTR LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "Commercial General Liability",
            "policy_0_policy_number": "GL-NEW",   # changed
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        })

    row = mock_corr_table.create.call_args[0][0]
    assert row["Source Filename"] == "cert01.json"
    assert row["Incoming Extraction Record ID"] == "recTEST"
    assert row["Old Value"] == "GL-OLD"
    assert row["New Value"] == "GL-NEW"
    assert row["Why (Haley's note)"] == "the reason"
    assert row["Learning Status"] == "New"
    assert "Model" in row
    assert "Corrected At" in row


# ── Processing Status never touched ──────────────────────────────────────────

def test_reject_never_touches_processing_status(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data={"action": "reject", "why": ""})

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Processing Status" not in fields_written
    assert fields_written["Review Status"] == "Rejected"


def test_escalate_never_touches_processing_status(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data={"action": "escalate", "why": ""})

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Processing Status" not in fields_written
    assert fields_written["Review Status"] == "Escalated to GC"


def test_approve_never_touches_processing_status(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data={
            "action": "approve", "why": "",
            "named_insured": "RTR LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "Commercial General Liability",
            "policy_0_policy_number": "GL-001",
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        })

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Processing Status" not in fields_written


# ── Prior Raw JSON never overwritten ─────────────────────────────────────────

def test_approve_never_overwrites_prior_raw_json(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    rec["fields"]["Prior Raw JSON (superseded)"] = '{"old": "data"}'
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data={
            "action": "approve", "why": "",
            "named_insured": "RTR LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "Commercial General Liability",
            "policy_0_policy_number": "GL-001",
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        })

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Prior Raw JSON (superseded)" not in fields_written
    from reextract_rtr import PRIOR_RAW_JSON_FIELD_ID
    assert PRIOR_RAW_JSON_FIELD_ID not in fields_written
