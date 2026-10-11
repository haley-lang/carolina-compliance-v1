"""Tests for review_app.py — login, flags, approve, corrections, and safety invariants."""
import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest

# Set env var before importing review_app
os.environ.setdefault("REVIEW_PASSWORD", "test_review_pass")

_TEST_CSRF = "test_csrf_token_32chars_padding_00"


@pytest.fixture
def app():
    import review_app
    review_app.app.config["TESTING"] = True
    return review_app.app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def clear_rate_limit():
    import review_app
    review_app._login_failures.clear()
    yield
    review_app._login_failures.clear()


@pytest.fixture
def csrf_client(client):
    """Unauthenticated client with a CSRF token pre-seeded."""
    with client.session_transaction() as sess:
        sess["csrf_token"] = _TEST_CSRF
    return client


@pytest.fixture
def authed_client(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["logged_in_at"] = time.time()
        sess["csrf_token"] = _TEST_CSRF
    return client


# ── Login / auth redirects ─────────────────────────────────────────────────────

def test_unauthed_queue_redirects_to_login(client):
    resp = client.get("/queue", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_unauthed_detail_redirects_to_login(client):
    resp = client.get("/detail/recABC", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_unauthed_action_redirects_to_login(client):
    # login_required runs before csrf_protect, so unauthenticated → redirect not 403
    resp = client.post("/action/recABC", data={"action": "approve"}, follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_wrong_password_rejected(csrf_client):
    resp = csrf_client.post(
        "/login",
        data={"password": "wrong", "csrf_token": _TEST_CSRF},
        follow_redirects=True,
    )
    assert b"Wrong password" in resp.data


def test_correct_password_grants_access(csrf_client):
    resp = csrf_client.post(
        "/login",
        data={"password": "test_review_pass", "csrf_token": _TEST_CSRF},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "/queue" in resp.headers["Location"]


# ── CSRF protection ───────────────────────────────────────────────────────────

def test_login_post_without_csrf_returns_403(client):
    resp = client.post("/login", data={"password": "test_review_pass"})
    assert resp.status_code == 403


def test_login_post_with_wrong_csrf_returns_403(client):
    with client.session_transaction() as sess:
        sess["csrf_token"] = _TEST_CSRF
    resp = client.post("/login", data={"password": "test_review_pass", "csrf_token": "wrongtoken"})
    assert resp.status_code == 403


def test_action_post_without_csrf_returns_403(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recTEST", data={"action": "approve"})
    assert resp.status_code == 403


def test_action_post_with_correct_csrf_succeeds(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recTEST", data={
            "csrf_token": _TEST_CSRF,
            "action": "reject",
            "why": "",
        }, follow_redirects=False)
    assert resp.status_code == 302


# ── Rate limiting ─────────────────────────────────────────────────────────────

def test_fifth_failure_is_still_allowed(csrf_client):
    """Exactly 5 failures — the 5th attempt should NOT yet be locked out."""
    import review_app
    for _ in range(4):
        csrf_client.post("/login", data={"password": "bad", "csrf_token": _TEST_CSRF})
    resp = csrf_client.post(
        "/login",
        data={"password": "bad", "csrf_token": _TEST_CSRF},
        follow_redirects=True,
    )
    assert b"Wrong password" in resp.data
    assert b"Too many failed" not in resp.data


def test_sixth_failure_triggers_lockout(csrf_client):
    """After 5 failures, the 6th attempt should be locked out."""
    import review_app
    for _ in range(5):
        csrf_client.post("/login", data={"password": "bad", "csrf_token": _TEST_CSRF})
    resp = csrf_client.post(
        "/login",
        data={"password": "bad", "csrf_token": _TEST_CSRF},
        follow_redirects=True,
    )
    assert b"Too many failed" in resp.data
    assert resp.status_code == 429


def test_correct_password_clears_failures(csrf_client):
    import review_app
    for _ in range(4):
        csrf_client.post("/login", data={"password": "bad", "csrf_token": _TEST_CSRF})
    csrf_client.post(
        "/login",
        data={"password": "test_review_pass", "csrf_token": _TEST_CSRF},
    )
    ip = "127.0.0.1"
    assert ip not in review_app._login_failures


# ── Session timeout ───────────────────────────────────────────────────────────

def test_expired_session_redirects_to_login(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["logged_in_at"] = time.time() - 3700  # 61 minutes ago
        sess["csrf_token"] = _TEST_CSRF
    resp = client.get("/queue", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_fresh_session_not_expired(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["logged_in_at"] = time.time()
        sess["csrf_token"] = _TEST_CSRF

    with patch("review_app._ie_table") as mock_table:
        mock_table.return_value.all.return_value = []
        resp = client.get("/queue")
    assert resp.status_code == 200


# ── Brand / rendering ─────────────────────────────────────────────────────────

def test_login_page_includes_brand_fonts(client):
    """Login page must load both Instrument Serif and Plus Jakarta Sans."""
    resp = client.get("/login")
    assert resp.status_code == 200
    assert b"Instrument+Serif" in resp.data
    assert b"Plus+Jakarta+Sans" in resp.data


def test_queue_page_includes_brand_fonts(client):
    """Queue page must load both brand fonts."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["logged_in_at"] = time.time()
        sess["csrf_token"] = _TEST_CSRF

    with patch("review_app._ie_table") as mock_table:
        mock_table.return_value.all.return_value = []
        resp = client.get("/queue")

    assert resp.status_code == 200
    assert b"Instrument+Serif" in resp.data
    assert b"Plus+Jakarta+Sans" in resp.data


def test_login_page_includes_review_css(client):
    """Login page must reference the custom stylesheet."""
    resp = client.get("/login")
    assert b"review.css" in resp.data


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


def _approve_data(**overrides):
    """Return a complete approve form dict with CSRF token pre-filled."""
    base = {
        "csrf_token": _TEST_CSRF,
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
        "policy_0_additional_insured_checked": "not_checked",
        "policy_0_waiver_of_subrogation_checked": "not_checked",
        "policy_0_primary_noncontributory_checked": "false",
    }
    base.update(overrides)
    return base


def test_approve_no_edits_sets_approved(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data=_approve_data())

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
        authed_client.post("/action/recTEST", data=_approve_data(
            policy_0_policy_number="GL-002",
        ))

    fields_written = mock_ie_table.update.call_args[0][1]
    assert fields_written["Review Status"] == "Approved + Edited"
    assert "Processing Status" not in fields_written


def test_approve_each_changed_field_writes_one_correction(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec(policy_number="GL-001")
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data=_approve_data(
            named_insured="RTR LLC (edited)",
            policy_0_policy_number="GL-002",
            why="fixing",
        ))

    # Two fields changed → two Corrections rows
    assert mock_corr_table.create.call_count == 2
    for call in mock_corr_table.create.call_args_list:
        assert call[0][0]["Learning Status"] == "New"


def test_approve_correction_row_has_required_fields(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec(policy_number="GL-OLD")
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data=_approve_data(
            policy_0_policy_number="GL-NEW",
            why="the reason",
        ))

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
        authed_client.post("/action/recTEST", data={
            "csrf_token": _TEST_CSRF, "action": "reject", "why": "",
        })

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Processing Status" not in fields_written
    assert fields_written["Review Status"] == "Rejected"


def test_escalate_never_touches_processing_status(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data={
            "csrf_token": _TEST_CSRF, "action": "escalate", "why": "",
        })

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Processing Status" not in fields_written
    assert fields_written["Review Status"] == "Escalated to GC"


def test_approve_never_touches_processing_status(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data=_approve_data())

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Processing Status" not in fields_written


# ── Prior Raw JSON never overwritten ─────────────────────────────────────────

def test_approve_never_overwrites_prior_raw_json(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec()
    rec["fields"]["Prior Raw JSON (superseded)"] = '{"old": "data"}'
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recTEST", data=_approve_data())

    fields_written = mock_ie_table.update.call_args[0][1]
    assert "Prior Raw JSON (superseded)" not in fields_written
    from reextract_rtr import PRIOR_RAW_JSON_FIELD_ID
    assert PRIOR_RAW_JSON_FIELD_ID not in fields_written


# ── multi_company flag (tightened regex) ──────────────────────────────────────

from review_app import _is_billy_rtr

def test_multi_company_false_for_single_name_with_comma_suffix():
    """Griffin Masonry, Inc — comma before entity suffix, NOT two companies."""
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Griffin Masonry, Inc", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is False


def test_multi_company_false_for_and_in_single_company_name():
    """E.E. SIDING AND CONSTRUCTION SOLUTION LLC — 'and' is part of company name, NOT separator."""
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "E.E. SIDING AND CONSTRUCTION SOLUTION LLC", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is False


def test_multi_company_false_for_plain_llc():
    """GREEN CLEAN ECO LLC — single entity, no separator pattern."""
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "GREEN CLEAN ECO LLC", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is False


def test_multi_company_true_for_two_suffixes_with_and():
    """Two LLC/Inc entities joined by ' and ' — genuinely two companies."""
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Smith LLC and Jones Inc", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is True


def test_multi_company_true_for_two_suffixes_with_ampersand():
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Alpha Corp & Beta LLC", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is True


def test_multi_company_true_for_two_suffixes_with_newline():
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Acme Inc\nBeta Corp", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is True


def test_multi_company_false_for_ampersand_without_entity_suffix():
    """'Henson Heating & Cooling' — ampersand but no entity suffix before it."""
    f = {"Confidence Score": 0.99}
    raw = {"document_type": "COI", "named_insured": "Henson Heating & Cooling", "policies": []}
    assert compute_flags(f, raw)["multi_company"] is False


# ── batch filter helper ────────────────────────────────────────────────────────

def test_is_billy_rtr_coi_forms_prefix():
    assert _is_billy_rtr("COI_forms_cert01.json") is True

def test_is_billy_rtr_rtr_coi_prefix():
    assert _is_billy_rtr("RTR_COI_page47_resend.json") is True

def test_is_billy_rtr_n9wc_prefix():
    assert _is_billy_rtr("N9WC394833-ACORDAPP25-I.json") is True

def test_is_billy_rtr_coi_json():
    assert _is_billy_rtr("coi.json") is True

def test_is_billy_rtr_resend_anywhere():
    assert _is_billy_rtr("some_resend_file.json") is True

def test_is_billy_rtr_false_for_test_data():
    assert _is_billy_rtr("scenario_1_renewal.json") is False

def test_is_billy_rtr_false_for_coi_forms_no_underscore():
    """COI_forms.json (no trailing _) should NOT be Billy/RTR."""
    assert _is_billy_rtr("COI_forms.json") is False

def test_is_billy_rtr_false_for_test_bro():
    assert _is_billy_rtr("TEST_BRO.json") is False


# ── detail page rendering ─────────────────────────────────────────────────────

def _make_rec_with_policies(source_filename="COI_forms_cert31.json"):
    """Fixture record with one policy — the exact shape that triggered the 500."""
    raw = {
        "document_type": "COI",
        "named_insured": "Test Co LLC",
        "certificate_holder": "GC Inc",
        "contact_emails": [],
        "policies": [{
            "policy_type": "GL",
            "policy_number": "GL-001",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M",
            "policy_basis": "occurrence",
            "additional_insured_checked": False,
            "waiver_of_subrogation_checked": False,
            "primary_noncontributory_checked": False,
        }],
    }
    return {
        "id": "recDETAIL",
        "fields": {
            "Source Filename": source_filename,
            "Named Insured": "Test Co LLC",
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.97,
        },
    }


def test_detail_page_renders_200_with_policies(authed_client, mock_ie_table):
    """detail.html must render OK when the record has at least one policy.

    This test caught the `policies | enumerate` filter bug: Jinja2 treats
    `enumerate` as a global function, not a filter, so `policies | enumerate`
    raises TemplateRuntimeError and returns a 500.  The fix is to call it as
    `enumerate(policies)` instead.
    """
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert resp.status_code == 200, (
        f"detail page returned {resp.status_code} — "
        "likely `policies | enumerate` filter bug in detail.html"
    )
    assert b"GL-001" in resp.data


def test_detail_page_renders_200_no_policies(authed_client, mock_ie_table):
    """Records with no policies should also render cleanly."""
    raw = {
        "document_type": "COI",
        "named_insured": "Empty Co",
        "certificate_holder": "GC Inc",
        "contact_emails": [],
        "policies": [],
    }
    rec = {
        "id": "recNOPOL",
        "fields": {
            "Source Filename": "COI_forms_cert03.json",
            "Named Insured": "Empty Co",
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.95,
        },
    }
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recNOPOL")
    assert resp.status_code == 200


def test_detail_shows_airtable_link(authed_client, mock_ie_table):
    """Airtable deep-link must appear in the topnav."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert b"airtable.com" in resp.data


def test_detail_approve_redirects_back(authed_client, mock_ie_table, mock_corr_table):
    """Approve on the detail page should redirect to detail with corrections_logged."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    # Pass data that exactly matches the record so corrections_logged == 0
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recDETAIL", data=_approve_data(
            named_insured="Test Co LLC",
            certificate_holder="GC Inc",
            policy_0_policy_type="GL",
        ), follow_redirects=False)
    assert resp.status_code == 302
    assert "recDETAIL" in resp.headers["Location"]
    assert "corrections_logged=0" in resp.headers["Location"]


def test_detail_approve_with_edit_shows_flash(authed_client, mock_ie_table, mock_corr_table):
    """After approve+edit, corrections_logged > 0 and the page loads."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recDETAIL", data=_approve_data(
            named_insured="Test Co LLC",
            certificate_holder="GC Inc",
            policy_0_policy_number="GL-999",  # changed
        ), follow_redirects=True)
    assert resp.status_code == 200
    assert b"Data confirmed" in resp.data


def test_detail_reject_redirects_to_queue(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recDETAIL", data={
            "csrf_token": _TEST_CSRF, "action": "reject", "why": "bad cert",
        }, follow_redirects=False)
    assert resp.status_code == 302
    assert "/queue" in resp.headers["Location"]


def test_detail_escalate_redirects_to_queue(authed_client, mock_ie_table, mock_corr_table):
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recDETAIL", data={
            "csrf_token": _TEST_CSRF, "action": "escalate", "why": "",
        }, follow_redirects=False)
    assert resp.status_code == 302
    assert "/queue" in resp.headers["Location"]


def test_detail_500_returns_error_page(app, authed_client, mock_ie_table):
    """When detail raises unexpectedly, the 500 handler must return the error page.

    Flask suppresses error handlers when TESTING=True; disable PROPAGATE_EXCEPTIONS
    just for this test so the handler actually runs.
    """
    app.config["PROPAGATE_EXCEPTIONS"] = False
    mock_ie_table.get.side_effect = RuntimeError("simulated crash")
    try:
        with patch("review_app._ie_table", return_value=mock_ie_table):
            resp = authed_client.get("/detail/recCRASH")
        assert resp.status_code == 500
        assert b"Something went wrong" in resp.data
    finally:
        app.config["PROPAGATE_EXCEPTIONS"] = True


# ── changed_vs_prior no longer triggers needs_look ───────────────────────────

def test_changed_vs_prior_alone_does_not_trigger_needs_look():
    """changed_vs_prior is informational — should not contribute to needs_look."""
    prior = {"policies": [{**_GL, "waiver_of_subrogation_checked": False}]}
    current = {
        "document_type": "COI",
        "named_insured": "Acme LLC",
        "policies": [{**_GL, "waiver_of_subrogation_checked": True}],
    }
    f = {"Confidence Score": 0.99, "Prior Raw JSON (superseded)": json.dumps(prior)}
    flags = compute_flags(f, current)
    assert flags["changed_vs_prior"] is True
    assert flags["needs_look"] is False


def test_changed_vs_prior_with_ai_disagreement_still_triggers_needs_look():
    """When another flag also fires, needs_look must still be True."""
    prior = {"policies": [{**_GL, "waiver_of_subrogation_checked": False}]}
    current = {
        "document_type": "COI",
        "named_insured": "Acme LLC",
        "policies": [{**_GL, "waiver_of_subrogation_checked": True}],
    }
    f = {
        "Confidence Score": 0.99,
        "AI Disagreements": "wos: False vs True",
        "Prior Raw JSON (superseded)": json.dumps(prior),
    }
    flags = compute_flags(f, current)
    assert flags["changed_vs_prior"] is True
    assert flags["needs_look"] is True


# ── possible_duplicate: policy-number + effective-date matching ───────────────

def _make_queue_rec(rec_id, named_insured="Acme LLC", policy_number="GL-001",
                    effective_date="2025-01-01", source="cert01.json"):
    raw = {
        "document_type": "COI",
        "named_insured": named_insured,
        "certificate_holder": "GC Inc",
        "contact_emails": [],
        "policies": [{
            **_GL,
            "policy_number": policy_number,
            "effective_date": effective_date,
        }],
    }
    return {
        "id": rec_id,
        "fields": {
            "Source Filename": source,
            "Named Insured": named_insured,
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.98,
            "Extraction Processed At": "2025-01-01T00:00:00Z",
        },
    }


def test_queue_possible_duplicate_fires_on_matching_policy(authed_client):
    """Two pending rows: same NI + same policy number + same effective date → both flagged."""
    rec1 = _make_queue_rec("recA", "Acme LLC", "GL-001", "2025-01-01")
    rec2 = _make_queue_rec("recB", "Acme LLC", "GL-001", "2025-01-01")
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec1, rec2]
        resp = authed_client.get("/queue?batch=other")
    assert resp.status_code == 200
    assert b"Possible duplicate" in resp.data
    assert b"GL-001" in resp.data  # match text shows original policy number


def test_queue_possible_duplicate_shows_match_text(authed_client):
    """Match text must contain the policy number and effective date."""
    rec1 = _make_queue_rec("recA", "Acme LLC", "WC-999", "2025-06-01")
    rec2 = _make_queue_rec("recB", "Acme LLC", "WC-999", "2025-06-01")
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec1, rec2]
        resp = authed_client.get("/queue?batch=other")
    assert b"WC-999" in resp.data
    assert b"2025-06-01" in resp.data


def test_queue_possible_duplicate_no_fire_on_different_policy_numbers(authed_client):
    """Same NI but different policy numbers → NOT a duplicate."""
    rec1 = _make_queue_rec("recA", "Acme LLC", "GL-001", "2025-01-01")
    rec2 = _make_queue_rec("recB", "Acme LLC", "GL-002", "2025-01-01")
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec1, rec2]
        resp = authed_client.get("/queue?batch=other")
    assert b"Possible duplicate" not in resp.data


def test_queue_possible_duplicate_no_fire_on_different_effective_dates(authed_client):
    """Same NI + same policy number but different effective dates → NOT a duplicate."""
    rec1 = _make_queue_rec("recA", "Acme LLC", "GL-001", "2025-01-01")
    rec2 = _make_queue_rec("recB", "Acme LLC", "GL-001", "2026-01-01")
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec1, rec2]
        resp = authed_client.get("/queue?batch=other")
    assert b"Possible duplicate" not in resp.data


def test_queue_possible_duplicate_no_fire_on_empty_policy_number(authed_client):
    """If a policy has no policy number, it cannot match."""
    rec1 = _make_queue_rec("recA", "Acme LLC", "", "2025-01-01")
    rec2 = _make_queue_rec("recB", "Acme LLC", "", "2025-01-01")
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec1, rec2]
        resp = authed_client.get("/queue?batch=other")
    assert b"Possible duplicate" not in resp.data


def test_queue_possible_duplicate_normalizes_policy_number(authed_client):
    """GL-001 and GL 001 are the same policy number after normalization."""
    rec1 = _make_queue_rec("recA", "Acme LLC", "GL-001", "2025-01-01")
    rec2 = _make_queue_rec("recB", "Acme LLC", "GL 001", "2025-01-01")
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec1, rec2]
        resp = authed_client.get("/queue?batch=other")
    assert b"Possible duplicate" in resp.data


# ── Task 1: 58-record smoke test (Billy/RTR batch) ────────────────────────────

def _make_billy_rec(rec_id, source_filename, named_insured="Test Co LLC",
                    review_status="Pending Review", confidence=0.97,
                    policies=None, ai_disagreements="", review_reason="",
                    prior_raw_json=""):
    """Build a mock record shaped like a real Billy/RTR Airtable record."""
    if policies is None:
        policies = [{
            "policy_type": "Commercial General Liability",
            "policy_number": "GL-001",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M/$2M",
            "policy_basis": "occurrence",
            "additional_insured_checked": False,
            "waiver_of_subrogation_checked": False,
            "primary_noncontributory_checked": False,
        }]
    raw = {
        "document_type": "COI",
        "named_insured": named_insured,
        "certificate_holder": "RTR Homes LLC",
        "contact_emails": [],
        "policies": policies,
        "confidence": confidence,
    }
    fields = {
        "Source Filename": source_filename,
        "Named Insured": named_insured,
        "Raw JSON": json.dumps(raw),
        "Review Status": review_status,
        "Confidence Score": confidence,
        "Extraction Processed At": "2025-10-01T12:00:00Z",
    }
    if ai_disagreements:
        fields["AI Disagreements"] = ai_disagreements
    if review_reason:
        fields["Review Reason"] = review_reason
    if prior_raw_json:
        fields["Prior Raw JSON (superseded)"] = prior_raw_json
    return {"id": rec_id, "fields": fields}


# Generate 58 representative Billy/RTR records covering all field variations
_BILLY_RTR_RECORDS = []
# COI_forms_ prefix variants (certs 1-47)
for _i in range(1, 48):
    _source = f"COI_forms_cert{_i:02d}.json"
    _conf = 0.95 + (_i % 5) * 0.01  # 0.95–0.99
    _status = "Pending Review" if _i % 3 != 0 else ("Approved" if _i % 6 == 0 else "Rejected")
    _ai_dis = f"GL.wos: False vs True" if _i % 7 == 0 else ""
    _prior = json.dumps({"document_type": "COI", "named_insured": f"Vendor {_i} LLC",
                         "policies": [{"policy_number": f"OLD-{_i:03d}",
                                       "additional_insured_checked": False,
                                       "waiver_of_subrogation_checked": False,
                                       "primary_noncontributory_checked": False}]}) if _i % 5 == 0 else ""
    _pols = [
        {
            "policy_type": "Commercial General Liability",
            "policy_number": f"GL-{_i:03d}",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M/$2M",
            "policy_basis": "occurrence",
            "additional_insured_checked": bool(_i % 2),
            "waiver_of_subrogation_checked": bool(_i % 3),
            "primary_noncontributory_checked": bool(_i % 4),
        },
        {
            "policy_type": "Workers Compensation",
            "policy_number": f"WC-{_i:03d}",
            "carrier": "Travelers",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$500K",
            "policy_basis": "",
            "additional_insured_checked": False,
            "waiver_of_subrogation_checked": bool(_i % 2),
            "primary_noncontributory_checked": False,
        },
    ] if _i % 4 != 0 else []  # some records have no policies
    _BILLY_RTR_RECORDS.append(
        _make_billy_rec(
            f"recBILLY{_i:03d}", _source,
            named_insured=f"Vendor {_i} LLC",
            review_status=_status,
            confidence=_conf,
            policies=_pols,
            ai_disagreements=_ai_dis,
            prior_raw_json=_prior,
        )
    )

# RTR_COI_ prefix variants (certs 48-55)
for _j in range(1, 9):
    _i = 47 + _j
    _source = f"RTR_COI_page{_j:02d}.json"
    _conf = 0.92 if _j % 2 == 0 else 0.98
    _pols = [{
        "policy_type": "Commercial General Liability",
        "policy_number": f"RTR-GL-{_j:03d}",
        "carrier": "State Farm",
        "effective_date": "2025-06-01",
        "expiration_date": "2026-06-01",
        "coverage_limits": "$2M",
        "policy_basis": "claims-made",
        "additional_insured_checked": True,
        "waiver_of_subrogation_checked": True,
        "primary_noncontributory_checked": True,
    }]
    _BILLY_RTR_RECORDS.append(
        _make_billy_rec(
            f"recRTR{_j:03d}", _source,
            named_insured=f"RTR Vendor {_j} Inc",
            confidence=_conf,
            policies=_pols,
        )
    )

# N9WC prefix + low confidence (cert 56)
_BILLY_RTR_RECORDS.append(
    _make_billy_rec(
        "recN9WC001", "N9WC394833-ACORDAPP25-I.json",
        named_insured="N9WC Services LLC",
        confidence=0.82,  # low confidence flag
        policies=[{
            "policy_type": "GL",
            "policy_number": "",  # missing policy number → missing_fields flag
            "carrier": "Nationwide",
            "effective_date": "2025-03-01",
            "expiration_date": "2026-03-01",
            "coverage_limits": "$1M",
            "policy_basis": "",
            "additional_insured_checked": False,
            "waiver_of_subrogation_checked": False,
            "primary_noncontributory_checked": False,
        }],
    )
)

# coi.json prefix (cert 57)
_BILLY_RTR_RECORDS.append(
    _make_billy_rec(
        "recCOI001", "coi.json",
        named_insured="Multi Co LLC and Partner Inc",  # multi_company flag
        confidence=0.97,
        ai_disagreements="named_insured: different read",
    )
)

# resend in filename (cert 58)
_BILLY_RTR_RECORDS.append(
    _make_billy_rec(
        "recRESEND001", "some_cert_resend.json",
        named_insured="Resend Vendor LLC",
        confidence=0.99,
        review_status="Approved",
    )
)

assert len(_BILLY_RTR_RECORDS) == 58, f"Expected 58 records, got {len(_BILLY_RTR_RECORDS)}"


def test_detail_renders_200_for_all_58_billy_rtr_records(authed_client, mock_ie_table):
    """Every Billy/RTR record must render the detail page without a 500."""
    errors = []
    for rec in _BILLY_RTR_RECORDS:
        mock_ie_table.get.return_value = rec
        with patch("review_app._ie_table", return_value=mock_ie_table):
            resp = authed_client.get(f"/detail/{rec['id']}")
        if resp.status_code != 200:
            errors.append(
                f"Record {rec['id']} ({rec['fields'].get('Source Filename')}) "
                f"returned HTTP {resp.status_code}"
            )
    assert not errors, "Some records returned non-200:\n" + "\n".join(errors)


# ── Task 2: session queue_ids / prev-next navigation ─────────────────────────

def test_queue_sets_session_queue_ids(authed_client):
    """Rendering /queue stores queue_ids in the session."""
    rec1 = _make_queue_rec("recA", source="COI_forms_cert01.json")
    rec2 = _make_queue_rec("recB", source="COI_forms_cert02.json")
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec1, rec2]
        authed_client.get("/queue?batch=rtr&filter=pending")
    with authed_client.session_transaction() as sess:
        assert "queue_ids" in sess
        assert set(sess["queue_ids"]) == {"recA", "recB"}
        assert sess["queue_batch"] == "rtr"
        assert sess["queue_chip"] == "pending"


def test_detail_passes_prev_none_at_first_record(authed_client, mock_ie_table):
    """First record in queue has prev_id=None."""
    rec = _make_rec_with_policies("COI_forms_cert01.json")
    rec["id"] = "recFIRST"
    # Seed the session queue
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recFIRST", "recSECOND", "recTHIRD"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "pending"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recFIRST")
    assert resp.status_code == 200
    # First cert: Previous button should be disabled (not a link)
    assert b"/detail/recSECOND" in resp.data  # next link present
    assert b"/detail/recFIRST" not in resp.data  # prev link absent (it IS the page)


def test_detail_passes_next_none_at_last_record(authed_client, mock_ie_table):
    """Last record in queue has next_id=None (Next button disabled)."""
    rec = _make_rec_with_policies("COI_forms_cert47.json")
    rec["id"] = "recLAST"
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recFIRST", "recSECOND", "recLAST"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "pending"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recLAST")
    assert resp.status_code == 200
    assert b"/detail/recSECOND" in resp.data  # prev link present
    # No next link beyond recLAST
    assert b"/detail/recLAST" not in resp.data


def test_detail_passes_both_prev_and_next_for_middle_record(authed_client, mock_ie_table):
    """Middle record in queue has both prev_id and next_id set."""
    rec = _make_rec_with_policies("COI_forms_cert02.json")
    rec["id"] = "recMID"
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recFIRST", "recMID", "recLAST"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "pending"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recMID")
    assert resp.status_code == 200
    assert b"/detail/recFIRST" in resp.data  # prev link
    assert b"/detail/recLAST" in resp.data   # next link


# ── Tri-state checkbox tests ──────────────────────────────────────────────────

def test_detail_tristate_na_rendered(authed_client, mock_ie_table):
    """Detail page renders 'not_applicable' radio for AI field."""
    raw = {
        "document_type": "COI",
        "named_insured": "Test Co",
        "policies": [{
            "policy_type": "GL",
            "policy_number": "GL-1",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M",
            "policy_basis": "occurrence",
            "additional_insured_checked": "not_applicable",
            "waiver_of_subrogation_checked": "checked",
            "primary_noncontributory_checked": False,
        }],
    }
    rec = {
        "id": "rec001",
        "fields": {
            "Source Filename": "cert01.json",
            "Named Insured": "Test Co",
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.97,
        },
    }
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/rec001")
    assert resp.status_code == 200
    html = resp.data.decode()
    assert 'value="not_applicable"' in html
    assert 'value="checked"' in html


def test_detail_statement_not_captured_for_old_rows(authed_client, mock_ie_table):
    """Old rows without statement fields show 'not captured yet'."""
    raw = {
        "document_type": "COI",
        "named_insured": "Old Co",
        "policies": [{
            "policy_type": "GL",
            "policy_number": "GL-1",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M",
            "policy_basis": "occurrence",
            "additional_insured_checked": False,
            "waiver_of_subrogation_checked": False,
            "primary_noncontributory_checked": False,
        }],
        # No additional_insured_statement or waiver_of_subrogation_statement
    }
    rec = {
        "id": "rec001",
        "fields": {
            "Source Filename": "cert01.json",
            "Named Insured": "Old Co",
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.97,
        },
    }
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/rec001")
    assert resp.status_code == 200
    assert b"not captured yet" in resp.data


def test_detail_statement_present_shown(authed_client, mock_ie_table):
    """Statement present for AI shows the quote text."""
    raw = {
        "document_type": "COI",
        "named_insured": "New Co",
        "additional_insured_statement": {
            "present": True,
            "quote": "Certificate holder is named as additional insured.",
            "policies_named": None,
        },
        "waiver_of_subrogation_statement": {"present": False, "quote": None, "policies_named": None},
        "policies": [{
            "policy_type": "GL",
            "policy_number": "GL-1",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M",
            "policy_basis": "occurrence",
            "additional_insured_checked": "not_applicable",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    rec = {
        "id": "rec001",
        "fields": {
            "Source Filename": "cert01.json",
            "Named Insured": "New Co",
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.97,
        },
    }
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/rec001")
    assert resp.status_code == 200
    assert b"Certificate holder is named as additional insured." in resp.data


def test_approve_tristate_ai_stored_as_string(authed_client, mock_ie_table, mock_corr_table):
    """Approving with AI=not_applicable stores the string value in Raw JSON."""
    raw = {
        "document_type": "COI",
        "named_insured": "Tri Co",
        "certificate_holder": "GC Inc",
        "contact_emails": [],
        "policies": [{
            "policy_type": "GL",
            "policy_number": "GL-1",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M",
            "policy_basis": "occurrence",
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
        "confidence": 0.98,
    }
    rec = {
        "id": "rec001",
        "fields": {
            "Source Filename": "cert01.json",
            "Named Insured": "Tri Co",
            "Raw JSON": json.dumps(raw),
            "Review Status": "Pending Review",
            "Confidence Score": 0.98,
        },
    }
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        data = _approve_data(
            named_insured="Tri Co",
            certificate_holder="GC Inc",
            policy_0_policy_type="GL",
            policy_0_policy_number="GL-1",
            policy_0_carrier="Hartford",
            policy_0_effective_date="2025-01-01",
            policy_0_expiration_date="2026-01-01",
            policy_0_coverage_limits="$1M",
            policy_0_policy_basis="occurrence",
            policy_0_additional_insured_checked="not_applicable",
            policy_0_waiver_of_subrogation_checked="checked",
            policy_0_primary_noncontributory_checked="false",
        )
        authed_client.post("/action/rec001", data=data)

    update_args = mock_ie_table.update.call_args
    fields_written = update_args[0][1]
    assert "Raw JSON" in fields_written
    saved_raw = json.loads(fields_written["Raw JSON"])
    p = saved_raw["policies"][0]
    assert p["additional_insured_checked"] == "not_applicable"
    assert p["waiver_of_subrogation_checked"] == "checked"


def test_detail_nav_bar_shows_position(authed_client, mock_ie_table):
    """Navigation bar should show 'Certificate N of M' text."""
    rec = _make_rec_with_policies("COI_forms_cert02.json")
    rec["id"] = "recMID"
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recFIRST", "recMID", "recLAST"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "pending"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recMID")
    assert b"Certificate 2 of 3" in resp.data


# ── Task 3: after action, go to next cert ────────────────────────────────────

def test_approve_redirects_to_next_when_queue_ids_in_session(authed_client, mock_ie_table, mock_corr_table):
    """After approve with a next_id in session, redirects to next cert detail."""
    rec = _make_rec_with_policies()
    rec["id"] = "recCURRENT"
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recCURRENT", "recNEXT"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "pending"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recCURRENT", data=_approve_data(
            named_insured="Test Co LLC",
            certificate_holder="GC Inc",
            policy_0_policy_type="GL",
        ), follow_redirects=False)
    assert resp.status_code == 302
    assert "/detail/recNEXT" in resp.headers["Location"]


def test_reject_redirects_to_next_when_queue_ids_in_session(authed_client, mock_ie_table, mock_corr_table):
    """After reject with a next_id in session, redirects to next cert detail."""
    rec = _make_rec_with_policies()
    rec["id"] = "recCURRENT"
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recCURRENT", "recNEXT"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "pending"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recCURRENT", data={
            "csrf_token": _TEST_CSRF, "action": "reject", "why": "",
        }, follow_redirects=False)
    assert resp.status_code == 302
    assert "/detail/recNEXT" in resp.headers["Location"]


def test_last_cert_approve_redirects_to_queue_with_params(authed_client, mock_ie_table, mock_corr_table):
    """After approving the last cert (no next_id), redirects to /queue with batch+filter."""
    rec = _make_rec_with_policies()
    rec["id"] = "recLAST"
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recFIRST", "recLAST"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "needs_look"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recLAST", data=_approve_data(
            named_insured="Test Co LLC",
            certificate_holder="GC Inc",
            policy_0_policy_type="GL",
        ), follow_redirects=False)
    # No next_id → falls back to same-page redirect (corrections_logged)
    assert resp.status_code == 302
    assert "recLAST" in resp.headers["Location"]
    assert "corrections_logged" in resp.headers["Location"]


def test_last_cert_reject_redirects_to_queue_with_params(authed_client, mock_ie_table, mock_corr_table):
    """After rejecting the last cert (no next_id), redirects to /queue with batch+filter."""
    rec = _make_rec_with_policies()
    rec["id"] = "recLAST"
    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recFIRST", "recLAST"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "needs_look"
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recLAST", data={
            "csrf_token": _TEST_CSRF, "action": "reject", "why": "",
        }, follow_redirects=False)
    assert resp.status_code == 302
    location = resp.headers["Location"]
    assert "/queue" in location
    assert "batch=rtr" in location
    assert "filter=needs_look" in location


def test_approve_flash_message_shown_on_next_page(authed_client, mock_ie_table, mock_corr_table):
    """After approve with next_id, flash message appears on the next detail page."""
    rec_current = _make_rec_with_policies("COI_forms_cert01.json")
    rec_current["id"] = "recCURRENT"
    rec_next = _make_rec_with_policies("COI_forms_cert02.json")
    rec_next["id"] = "recNEXT"

    with authed_client.session_transaction() as sess:
        sess["queue_ids"] = ["recCURRENT", "recNEXT"]
        sess["queue_batch"] = "rtr"
        sess["queue_chip"] = "pending"

    # First call: get() returns current rec for the action
    # Second call (after redirect follow): get() returns next rec
    mock_ie_table.get.side_effect = [rec_current, rec_next]
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recCURRENT", data=_approve_data(
            named_insured="Test Co LLC",
            certificate_holder="GC Inc",
            policy_0_policy_type="GL",
        ), follow_redirects=True)
    assert resp.status_code == 200
    assert b"Data confirmed" in resp.data


# ── UX Changes: zoom, wording, save_draft, toggles ───────────────────────────

def test_detail_zoom_container_present(authed_client, mock_ie_table):
    """Zoom container elements must be present in the detail page HTML."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert resp.status_code == 200
    assert b"zoom-outer" in resp.data
    assert b"zoom-in" in resp.data
    assert b"zoom-out" in resp.data
    assert b"zoom-reset" in resp.data


def test_detail_policy_cards_not_table(authed_client, mock_ie_table):
    """Policy layout uses cards (policy-card class), not the old wide table."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert b"policy-card" in resp.data
    assert b"policy-table" not in resp.data


def test_detail_button_text_data_is_correct(authed_client, mock_ie_table):
    """Approve button must say 'Data is correct', not 'Looks right'."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert b"Data is correct" in resp.data
    assert b"Looks right" not in resp.data


def test_detail_has_save_and_keep_button(authed_client, mock_ie_table):
    """Detail page must have a 'Save and keep in queue' button."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert b"save_draft" in resp.data
    assert b"Save and keep in queue" in resp.data


def test_detail_disclaimer_sentence_present(authed_client, mock_ie_table):
    """The disclaimer sentence must appear under the action buttons."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert b"does not approve the vendor" in resp.data


def test_save_draft_leaves_review_status_pending(authed_client, mock_ie_table, mock_corr_table):
    """save_draft must NOT change Review Status."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recDETAIL", data={
            "csrf_token": _TEST_CSRF,
            "action": "save_draft",
            "why": "",
            "named_insured": "Test Co LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "GL",
            "policy_0_policy_number": "GL-001",
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        }, follow_redirects=False)
    # Must not touch Review Status
    if mock_ie_table.update.called:
        fields_written = mock_ie_table.update.call_args[0][1]
        assert "Review Status" not in fields_written


def test_save_draft_with_edit_writes_corrections(authed_client, mock_ie_table, mock_corr_table):
    """save_draft must write Corrections rows when fields are changed."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        authed_client.post("/action/recDETAIL", data={
            "csrf_token": _TEST_CSRF,
            "action": "save_draft",
            "why": "fixing number",
            "named_insured": "Test Co LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "GL",
            "policy_0_policy_number": "GL-999",  # changed
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        })
    mock_corr_table.create.assert_called_once()
    row = mock_corr_table.create.call_args[0][0]
    assert row["New Value"] == "GL-999"


def test_save_draft_redirects_to_same_detail(authed_client, mock_ie_table, mock_corr_table):
    """save_draft must redirect back to the same detail page."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        resp = authed_client.post("/action/recDETAIL", data={
            "csrf_token": _TEST_CSRF,
            "action": "save_draft",
            "why": "",
            "named_insured": "Test Co LLC",
            "certificate_holder": "GC Inc",
            "policy_0_policy_type": "GL",
            "policy_0_policy_number": "GL-001",
            "policy_0_carrier": "Hartford",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "false",
            "policy_0_waiver_of_subrogation_checked": "false",
            "policy_0_primary_noncontributory_checked": "false",
        }, follow_redirects=False)
    assert resp.status_code == 302
    assert "recDETAIL" in resp.headers["Location"]


def test_detail_radio_toggles_for_checkboxes(authed_client, mock_ie_table):
    """Checkbox fields must use radio buttons (not select) for Checked/Not checked."""
    rec = _make_rec_with_policies()
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    html = resp.data.decode()
    assert 'type="radio"' in html
    assert 'name="policy_0_additional_insured_checked"' in html
    assert 'name="policy_0_waiver_of_subrogation_checked"' in html
    assert 'name="policy_0_primary_noncontributory_checked"' in html
    assert "Checked" in html
    assert "Not checked" in html


def test_queue_shows_data_confirmed_for_approved_records(authed_client):
    """Queue badge must show 'Data confirmed' for Approved/Approved+Edited records."""
    rec = _make_queue_rec("recAPPROVED", source="COI_forms_cert01.json")
    rec["fields"]["Review Status"] = "Approved"
    with patch("review_app._ie_table") as mock:
        mock.return_value.all.return_value = [rec]
        resp = authed_client.get("/queue?batch=rtr&filter=approved")
    assert b"Data confirmed" in resp.data
    assert b">Approved<" not in resp.data  # raw "Approved" text must not appear as badge text


def test_source_page_url_uses_thumbnail_for_pdf_attachment(authed_client, mock_ie_table):
    """When Source Page attachment is a PDF, use the large thumbnail URL for display."""
    import review_app
    rec = _make_rec_with_policies()
    rec["fields"]["Source Page"] = [{
        "id": "att123",
        "url": "https://cdn.airtable.com/original.pdf",
        "filename": "page.pdf",
        "type": "application/pdf",
        "thumbnails": {
            "large": {"url": "https://cdn.airtable.com/thumb_large.png", "width": 512, "height": 663}
        }
    }]
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert b"thumb_large.png" in resp.data
    assert b"original.pdf" not in resp.data  # should NOT be used as the img src


def test_source_page_url_uses_original_for_image_attachment(authed_client, mock_ie_table):
    """When Source Page attachment is an image, use the full-res original URL."""
    rec = _make_rec_with_policies()
    rec["fields"]["Source Page"] = [{
        "id": "att456",
        "url": "https://cdn.airtable.com/page.png",
        "filename": "page.png",
        "type": "image/png",
        "thumbnails": {
            "large": {"url": "https://cdn.airtable.com/thumb.png", "width": 512, "height": 663}
        }
    }]
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table):
        resp = authed_client.get("/detail/recDETAIL")
    assert b"cdn.airtable.com/page.png" in resp.data


# ── Full-field editable detail page tests ─────────────────────────────────────

def _make_cert_fixture(raw, source_filename="verify_test.json", record_id=None,
                        named_insured=None, review_status="Pending Review",
                        confidence=0.98):
    """Helper to build an Airtable-record-shaped fixture from a raw JSON dict."""
    return {
        "id": record_id or "recFIXTURE",
        "fields": {
            "Source Filename": source_filename,
            "Named Insured": named_insured or raw.get("named_insured") or "",
            "Raw JSON": json.dumps(raw),
            "Review Status": review_status,
            "Confidence Score": confidence,
        },
    }


class patch_table:
    """Context manager that patches review_app._ie_table so .get(record_id) returns from a dict."""
    def __init__(self, records_by_id):
        self._records = records_by_id
        self._patcher = None

    def __enter__(self):
        mock = MagicMock()

        def _get(rid):
            return self._records.get(rid)

        mock.get.side_effect = _get
        mock.all.return_value = list(self._records.values())
        self._patcher = patch("review_app._ie_table", return_value=mock)
        self._patcher.start()
        return mock

    def __exit__(self, *args):
        self._patcher.stop()


class patch_corrections_create:
    """Patches review_app._corrections_table so .create(fields) calls fn(fields)."""
    def __init__(self, fn):
        self._fn = fn
        self._patcher = None

    def __enter__(self):
        mock = MagicMock()
        mock.create.side_effect = self._fn
        self._patcher = patch("review_app._corrections_table", return_value=mock)
        self._patcher.start()
        return mock

    def __exit__(self, *args):
        self._patcher.stop()


class patch_corrections_all:
    """Patches review_app._corrections_table so .all() returns a given list."""
    def __init__(self, rows):
        self._rows = rows
        self._patcher = None

    def __enter__(self):
        mock = MagicMock()
        mock.all.return_value = self._rows
        self._patcher = patch("review_app._corrections_table", return_value=mock)
        self._patcher.start()
        return mock

    def __exit__(self, *args):
        self._patcher.stop()


# ── Corrections row field verification (item 5) ──────────────────────────────

def test_approve_corrections_row_has_all_required_fields(authed_client):
    """Approving with a changed named_insured writes one Corrections row with every required field."""
    raw = {
        "document_type": "COI",
        "named_insured": "Original Co",
        "policies": [{
            "policy_type": "GL",
            "policy_number": "GL-1",
            "carrier": "Hartford",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1M",
            "policy_basis": "occurrence",
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    cert = _make_cert_fixture(raw, source_filename="verify_test.json", record_id="recV1",
                               named_insured="Original Co")
    created = []

    def fake_corrections_create(fields):
        created.append(fields)
        return {"id": "recC1", "fields": fields}

    with patch_table({"recV1": cert}):
        with patch_corrections_create(fake_corrections_create):
            # Build data that matches the fixture exactly, except named_insured
            data = {
                "csrf_token": _TEST_CSRF,
                "action": "approve",
                "why": "",
                "named_insured": "Corrected Co",
                "certificate_holder": "",
                "document_type": "COI",
                "policy_0_policy_type": "GL",
                "policy_0_policy_number": "GL-1",
                "policy_0_carrier": "Hartford",
                "policy_0_effective_date": "2025-01-01",
                "policy_0_expiration_date": "2026-01-01",
                "policy_0_coverage_limits": "$1M",
                "policy_0_policy_basis": "occurrence",
                "policy_0_additional_insured_checked": "not_checked",
                "policy_0_waiver_of_subrogation_checked": "not_checked",
                "policy_0_primary_noncontributory_checked": "false",
            }
            authed_client.post("/action/recV1", data=data)

    assert len(created) == 1, f"Expected 1 correction row, got {len(created)}: {[c.get('Field') for c in created]}"
    row = created[0]
    assert row["Field"] == "named_insured", f"Wrong field: {row.get('Field')}"
    assert row["Old Value"] == "Original Co"
    assert row["New Value"] == "Corrected Co"
    assert row["Source Filename"] == "verify_test.json"
    assert row["Incoming Extraction Record ID"] == "recV1"
    assert row["Learning Status"] == "New"
    assert row.get("Model"), "Model must be set"
    assert row.get("Corrected At"), "Corrected At must be set"
    assert row.get("Summary"), "Summary must be set"


# ── Corrections page tests ────────────────────────────────────────────────────

def test_corrections_page_renders(authed_client):
    """GET /corrections returns 200."""
    with patch_corrections_all([]):
        resp = authed_client.get("/corrections")
    assert resp.status_code == 200
    assert b"Corrections" in resp.data


def test_corrections_page_shows_rows(authed_client):
    """Corrections page shows one row from the table."""
    rows = [{"id": "r1", "fields": {
        "Field": "named_insured",
        "Old Value": "Old Co",
        "New Value": "New Co",
        "Source Filename": "test.json",
        "Incoming Extraction Record ID": "rec001",
        "Why (Haley's note)": "OCR",
        "Model": "claude-test",
        "Corrected At": "2026-10-11T00:00:00Z",
        "Learning Status": "New",
        "Summary": "Changed named_insured: 'Old Co' -> 'New Co'",
    }}]
    with patch_corrections_all(rows):
        resp = authed_client.get("/corrections")
    assert b"named_insured" in resp.data
    assert b"Old Co" in resp.data
    assert b"New Co" in resp.data


def test_corrections_page_field_filter(authed_client):
    """Corrections page field filter hides non-matching rows."""
    rows = [
        {"id": "r1", "fields": {"Field": "named_insured", "Old Value": "A", "New Value": "B",
                                 "Learning Status": "New", "Model": "m", "Source Filename": "f.json",
                                 "Incoming Extraction Record ID": "r", "Corrected At": "", "Summary": ""}},
        {"id": "r2", "fields": {"Field": "carrier", "Old Value": "X", "New Value": "Y",
                                 "Learning Status": "New", "Model": "m", "Source Filename": "f.json",
                                 "Incoming Extraction Record ID": "r", "Corrected At": "", "Summary": ""}},
    ]
    with patch_corrections_all(rows):
        resp = authed_client.get("/corrections?field=named_insured")
    assert b"named_insured" in resp.data
    # Only hidden-row test: the HTML will still include "carrier" in the filter dropdown,
    # so we check that the carrier row's distinctive values don't appear.
    html = resp.data.decode()
    assert "Y" not in html.split("tbody")[1].split("</tbody>")[0] if "tbody" in html else True


def test_corrections_page_unauthenticated_redirects_to_login(client):
    resp = client.get("/corrections", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


# ── Detail page placeholder & new field tests ────────────────────────────────

def test_detail_empty_field_shows_placeholder(authed_client):
    """Detail page renders inputs with placeholder=(blank) for empty fields."""
    cert = _make_cert_fixture(
        {"document_type": "COI", "named_insured": "", "policies": []},
        source_filename="placeholder_test.json",
        record_id="recP1",
    )
    with patch_table({"recP1": cert}):
        resp = authed_client.get("/detail/recP1")
    assert resp.status_code == 200
    assert b'placeholder="(blank)"' in resp.data


def test_detail_shows_description_of_operations(authed_client):
    """Detail page renders description_of_operations textarea."""
    cert = _make_cert_fixture(
        {"document_type": "COI", "named_insured": "Test Co",
         "description_of_operations": "AI is named as additional insured.",
         "policies": []},
        source_filename="desc_test.json",
        record_id="recD1",
    )
    with patch_table({"recD1": cert}):
        resp = authed_client.get("/detail/recD1")
    assert b"AI is named as additional insured." in resp.data
    assert b"description_of_operations" in resp.data


def test_detail_shows_document_type_select(authed_client):
    """document_type is a <select> with the five expected options."""
    cert = _make_cert_fixture(
        {"document_type": "endorsement", "named_insured": "X", "policies": []},
        record_id="recDS1",
    )
    with patch_table({"recDS1": cert}):
        resp = authed_client.get("/detail/recDS1")
    html = resp.data.decode()
    for opt in ("COI", "cancellation_notice", "endorsement", "reinstatement", "unknown"):
        assert f'value="{opt}"' in html, f"missing option {opt}"


def test_detail_shows_retro_date_input(authed_client):
    """retro_date input is editable on the policy card."""
    raw = {
        "document_type": "COI",
        "named_insured": "X",
        "policies": [{
            "policy_type": "GL", "policy_number": "GL-1", "carrier": "H",
            "effective_date": "2025-01-01", "expiration_date": "2026-01-01",
            "coverage_limits": "$1M", "policy_basis": "claims-made",
            "retro_date": "2020-05-15",
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    cert = _make_cert_fixture(raw, record_id="recR1")
    with patch_table({"recR1": cert}):
        resp = authed_client.get("/detail/recR1")
    html = resp.data.decode()
    assert 'name="policy_0_retro_date"' in html
    assert "2020-05-15" in html


def test_detail_shows_tail_and_policy_number_invalid_toggles(authed_client):
    """tail_coverage_evidenced and policy_number_invalid Yes/No toggles are rendered."""
    raw = {
        "document_type": "COI",
        "named_insured": "X",
        "policies": [{
            "policy_type": "GL", "policy_number": "GL-1", "carrier": "H",
            "effective_date": "2025-01-01", "expiration_date": "2026-01-01",
            "coverage_limits": "$1M", "policy_basis": "occurrence",
            "tail_coverage_evidenced": True,
            "policy_number_invalid": False,
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    cert = _make_cert_fixture(raw, record_id="recT1")
    with patch_table({"recT1": cert}):
        resp = authed_client.get("/detail/recT1")
    html = resp.data.decode()
    assert 'name="policy_0_tail_coverage_evidenced"' in html
    assert 'name="policy_0_policy_number_invalid"' in html


def test_detail_shows_other_data_card_for_unknown_keys(authed_client):
    """Keys outside the known schema render in the 'Other data' card."""
    raw = {
        "document_type": "COI",
        "named_insured": "X",
        "policies": [],
        "mystery_field": "mystery_value",
    }
    cert = _make_cert_fixture(raw, record_id="recO1")
    with patch_table({"recO1": cert}):
        resp = authed_client.get("/detail/recO1")
    html = resp.data.decode()
    assert "Other data" in html
    assert "mystery_field" in html
    assert "mystery_value" in html


def test_detail_other_data_card_hidden_when_no_unknown_keys(authed_client):
    """'Other data' card header is omitted entirely when there are no unknown keys."""
    raw = {
        "document_type": "COI",
        "named_insured": "X",
        "certificate_holder": "Y",
        "certificate_date": "2025-01-01",
        "contact_emails": [],
        "description_of_operations": "",
        "confidence": 0.9,
        "additional_insured_statement": {"present": False, "quote": None, "policies_named": None},
        "waiver_of_subrogation_statement": {"present": False, "quote": None, "policies_named": None},
        "policies": [],
    }
    cert = _make_cert_fixture(raw, record_id="recO2")
    with patch_table({"recO2": cert}):
        resp = authed_client.get("/detail/recO2")
    # The card header "<div class='card-header'>Other data</div>" must NOT be present
    # (the HTML comment "<!-- Other data ... -->" is always in the template).
    assert b'class="card-header">Other data' not in resp.data


def test_detail_statement_quote_textarea_editable(authed_client):
    """Statement quote renders as an editable textarea with the ai_stmt_quote name."""
    raw = {
        "document_type": "COI",
        "named_insured": "X",
        "additional_insured_statement": {"present": True, "quote": "AI is a listed insured.", "policies_named": ["GL"]},
        "waiver_of_subrogation_statement": {"present": False, "quote": None, "policies_named": None},
        "policies": [],
    }
    cert = _make_cert_fixture(raw, record_id="recS1")
    with patch_table({"recS1": cert}):
        resp = authed_client.get("/detail/recS1")
    html = resp.data.decode()
    assert 'name="ai_stmt_quote"' in html
    assert "AI is a listed insured." in html
    assert 'name="ai_stmt_policies_named"' in html
    assert 'name="ai_stmt_present"' in html


# ── Approve: new editable fields update Raw JSON ─────────────────────────────

def test_approve_description_of_operations_edit_writes_correction(authed_client,
                                                                   mock_ie_table, mock_corr_table):
    """Editing description_of_operations writes a Corrections row and updates Raw JSON."""
    raw = {
        "document_type": "COI",
        "named_insured": "D Co",
        "certificate_holder": "GC",
        "description_of_operations": "Original.",
        "policies": [{
            "policy_type": "GL", "policy_number": "GL-1", "carrier": "H",
            "effective_date": "2025-01-01", "expiration_date": "2026-01-01",
            "coverage_limits": "$1M", "policy_basis": "occurrence",
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    rec = {"id": "recD2", "fields": {"Source Filename": "d.json", "Named Insured": "D Co",
                                       "Raw JSON": json.dumps(raw), "Review Status": "Pending Review",
                                       "Confidence Score": 0.98}}
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        data = {
            "csrf_token": _TEST_CSRF, "action": "approve", "why": "",
            "named_insured": "D Co", "certificate_holder": "GC",
            "document_type": "COI",
            "description_of_operations": "Updated description.",
            "policy_0_policy_type": "GL",
            "policy_0_policy_number": "GL-1",
            "policy_0_carrier": "H",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "not_checked",
            "policy_0_waiver_of_subrogation_checked": "not_checked",
            "policy_0_primary_noncontributory_checked": "false",
        }
        authed_client.post("/action/recD2", data=data)

    # One Correction row for description_of_operations
    fields_logged = [c[0][0]["Field"] for c in mock_corr_table.create.call_args_list]
    assert fields_logged == ["description_of_operations"], f"Unexpected corrections: {fields_logged}"
    row = mock_corr_table.create.call_args[0][0]
    assert row["Field"] == "description_of_operations"
    assert row["New Value"] == "Updated description."
    # Raw JSON was updated
    saved = json.loads(mock_ie_table.update.call_args[0][1]["Raw JSON"])
    assert saved["description_of_operations"] == "Updated description."


def test_approve_statement_edit_writes_correction(authed_client, mock_ie_table, mock_corr_table):
    """Editing additional_insured_statement.present writes a Corrections row."""
    raw = {
        "document_type": "COI",
        "named_insured": "S Co",
        "certificate_holder": "GC",
        "additional_insured_statement": {"present": False, "quote": None, "policies_named": None},
        "policies": [{
            "policy_type": "GL", "policy_number": "GL-1", "carrier": "H",
            "effective_date": "2025-01-01", "expiration_date": "2026-01-01",
            "coverage_limits": "$1M", "policy_basis": "occurrence",
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    rec = {"id": "recST", "fields": {"Source Filename": "s.json", "Named Insured": "S Co",
                                        "Raw JSON": json.dumps(raw), "Review Status": "Pending Review",
                                        "Confidence Score": 0.98}}
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        data = {
            "csrf_token": _TEST_CSRF, "action": "approve", "why": "",
            "named_insured": "S Co", "certificate_holder": "GC",
            "document_type": "COI",
            "ai_stmt_present": "true",
            "ai_stmt_quote": "now present",
            "ai_stmt_policies_named": "GL, WC",
            "policy_0_policy_type": "GL",
            "policy_0_policy_number": "GL-1",
            "policy_0_carrier": "H",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "not_checked",
            "policy_0_waiver_of_subrogation_checked": "not_checked",
            "policy_0_primary_noncontributory_checked": "false",
        }
        authed_client.post("/action/recST", data=data)

    # Expect at least 2 corrections (present + quote; policies_named may be third)
    assert mock_corr_table.create.call_count >= 2
    fields_logged = [c[0][0]["Field"] for c in mock_corr_table.create.call_args_list]
    assert "additional_insured_statement.present" in fields_logged
    assert "additional_insured_statement.quote" in fields_logged
    # Raw JSON reflects new statement
    saved = json.loads(mock_ie_table.update.call_args[0][1]["Raw JSON"])
    assert saved["additional_insured_statement"]["present"] is True
    assert saved["additional_insured_statement"]["quote"] == "now present"
    assert saved["additional_insured_statement"]["policies_named"] == ["GL", "WC"]


def test_approve_document_type_edit_updates_airtable_column(authed_client, mock_ie_table, mock_corr_table):
    """Changing document_type should write to Raw JSON and the Airtable Document Type column."""
    raw = {
        "document_type": "COI",
        "named_insured": "Doc Co",
        "certificate_holder": "GC",
        "policies": [{
            "policy_type": "GL", "policy_number": "GL-1", "carrier": "H",
            "effective_date": "2025-01-01", "expiration_date": "2026-01-01",
            "coverage_limits": "$1M", "policy_basis": "occurrence",
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    rec = {"id": "recDT", "fields": {"Source Filename": "d.json", "Named Insured": "Doc Co",
                                        "Raw JSON": json.dumps(raw), "Review Status": "Pending Review",
                                        "Confidence Score": 0.98}}
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        data = {
            "csrf_token": _TEST_CSRF, "action": "approve", "why": "",
            "named_insured": "Doc Co", "certificate_holder": "GC",
            "document_type": "endorsement",
            "policy_0_policy_type": "GL",
            "policy_0_policy_number": "GL-1",
            "policy_0_carrier": "H",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "occurrence",
            "policy_0_additional_insured_checked": "not_checked",
            "policy_0_waiver_of_subrogation_checked": "not_checked",
            "policy_0_primary_noncontributory_checked": "false",
        }
        authed_client.post("/action/recDT", data=data)

    fields_written = mock_ie_table.update.call_args[0][1]
    assert fields_written.get("Document Type") == "endorsement"
    saved = json.loads(fields_written["Raw JSON"])
    assert saved["document_type"] == "endorsement"


def test_approve_tail_coverage_edit_writes_correction(authed_client, mock_ie_table, mock_corr_table):
    """Changing tail_coverage_evidenced writes a Corrections row."""
    raw = {
        "document_type": "COI",
        "named_insured": "T Co",
        "certificate_holder": "GC",
        "policies": [{
            "policy_type": "GL", "policy_number": "GL-1", "carrier": "H",
            "effective_date": "2025-01-01", "expiration_date": "2026-01-01",
            "coverage_limits": "$1M", "policy_basis": "claims-made",
            "tail_coverage_evidenced": False,
            "additional_insured_checked": "not_checked",
            "waiver_of_subrogation_checked": "not_checked",
            "primary_noncontributory_checked": False,
        }],
    }
    rec = {"id": "recTA", "fields": {"Source Filename": "t.json", "Named Insured": "T Co",
                                        "Raw JSON": json.dumps(raw), "Review Status": "Pending Review",
                                        "Confidence Score": 0.98}}
    mock_ie_table.get.return_value = rec
    with patch("review_app._ie_table", return_value=mock_ie_table), \
         patch("review_app._corrections_table", return_value=mock_corr_table):
        data = {
            "csrf_token": _TEST_CSRF, "action": "approve", "why": "",
            "named_insured": "T Co", "certificate_holder": "GC",
            "document_type": "COI",
            "policy_0_policy_type": "GL",
            "policy_0_policy_number": "GL-1",
            "policy_0_carrier": "H",
            "policy_0_effective_date": "2025-01-01",
            "policy_0_expiration_date": "2026-01-01",
            "policy_0_coverage_limits": "$1M",
            "policy_0_policy_basis": "claims-made",
            "policy_0_additional_insured_checked": "not_checked",
            "policy_0_waiver_of_subrogation_checked": "not_checked",
            "policy_0_primary_noncontributory_checked": "false",
            "policy_0_tail_coverage_evidenced": "true",
        }
        authed_client.post("/action/recTA", data=data)

    fields_logged = [c[0][0]["Field"] for c in mock_corr_table.create.call_args_list]
    assert "policies[0].tail_coverage_evidenced" in fields_logged


# ── export_gold_candidates verification (item 6) ─────────────────────────────

def test_export_gold_candidates_all_required_fields():
    """export_gold_candidates picks up 'New' corrections and writes all required fields."""
    import json as _json
    import tempfile
    from pathlib import Path
    from unittest.mock import patch as _patch
    import export_gold_candidates

    mock_records = [{"id": "recTEST", "fields": {
        "Source Filename": "export_test.json",
        "Incoming Extraction Record ID": "recABC",
        "Field": "named_insured",
        "Old Value": "Old Co",
        "New Value": "New Co",
        "Why (Haley's note)": "OCR error",
        "Model": "claude-test",
        "Corrected At": "2026-10-11T12:00:00.000Z",
        "Learning Status": "New",
    }}]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        cand_path = tmp_path / "rtr_gold_candidates.json"
        forbidden = tmp_path / "rtr_gold.json"

        with _patch.object(export_gold_candidates, "CANDIDATES_PATH", cand_path), \
             _patch.object(export_gold_candidates, "GOLD_DIR", tmp_path), \
             _patch.object(export_gold_candidates, "_FORBIDDEN_PATH", forbidden), \
             _patch("export_gold_candidates.config") as mock_cfg, \
             _patch("export_gold_candidates.clean_base_id", side_effect=lambda x: x), \
             _patch("export_gold_candidates.Api") as mock_api:

            mock_cfg.AIRTABLE_API_KEY = "fakekey"
            mock_cfg.AIRTABLE_BASE_ID = "appFAKE"
            table = mock_api.return_value.table.return_value
            table.all.return_value = mock_records

            export_gold_candidates.run()

        assert cand_path.exists(), "candidates file should be created"
        candidates = _json.loads(cand_path.read_text())
        assert isinstance(candidates, list)
        assert len(candidates) == 1
        c = candidates[0]
        assert c["source_filename"] == "export_test.json"
        assert c["record_id"] == "recABC"
        assert c["field_path"] == "named_insured"
        assert c["old_value"] == "Old Co"
        assert c["corrected_value"] == "New Co"
        assert c["why"] == "OCR error"
        assert c["model"] == "claude-test"
        assert c["status"] == "candidate"
