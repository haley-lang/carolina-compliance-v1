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
        "policy_0_additional_insured_checked": "false",
        "policy_0_waiver_of_subrogation_checked": "false",
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
    assert b"Corrections logged" in resp.data


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
