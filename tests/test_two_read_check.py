"""Tests for the two-read quality-assurance check (Part A)."""
import json
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch
from pathlib import Path

import pytest

import extractor
from airtable_importer import (
    build_fields,
    SECOND_READ_JSON_FIELD_ID,
    AI_DISAGREEMENTS_FIELD_ID,
)
from review_gate import (
    compute_review_status,
    REVIEW_STATUS_PENDING_REVIEW,
    REVIEW_STATUS_AUTO_APPROVED,
    REVIEW_REASON_AI_DISAGREEMENT,
    REVIEW_REASON_POSSIBLE_DUPLICATE,
    REVIEW_REASON_NA,
    PROCESSING_STATUS_NEEDS_REVIEW,
    PROCESSING_STATUS_DUPLICATE,
    PROCESSING_STATUS_IMPORTED,
)
import reextract_rtr


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_ext(doc_type="COI", named_insured="Acme LLC", policies=None):
    return {
        "document_type": doc_type,
        "named_insured": named_insured,
        "policies": policies or [],
        "confidence": 0.98,
    }


def _gl_policy(**overrides):
    base = {
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
    base.update(overrides)
    return base


def _mock_response(data_dict):
    return NS(content=[NS(type="text", text=json.dumps(data_dict))])


# ── _compare_reads ────────────────────────────────────────────────────────────

def test_compare_reads_identical_returns_empty():
    a = _make_ext(policies=[_gl_policy()])
    assert extractor._compare_reads(a, a) == []


def test_compare_reads_named_insured_diff():
    a = _make_ext(named_insured="Acme LLC")
    b = _make_ext(named_insured="ACME, LLC")
    diffs = extractor._compare_reads(a, b)
    assert any("named_insured" in d for d in diffs)


def test_compare_reads_named_insured_case_insensitive():
    a = _make_ext(named_insured="Acme LLC")
    b = _make_ext(named_insured="acme llc")
    diffs = extractor._compare_reads(a, b)
    assert not any("named_insured" in d for d in diffs)


def test_compare_reads_wos_diff():
    a = _make_ext(policies=[_gl_policy(waiver_of_subrogation_checked=False)])
    b = _make_ext(policies=[_gl_policy(waiver_of_subrogation_checked=True)])
    diffs = extractor._compare_reads(a, b)
    assert any("wos" in d for d in diffs)


def test_compare_reads_document_type_diff():
    a = _make_ext(doc_type="COI")
    b = _make_ext(doc_type="cancellation_notice")
    diffs = extractor._compare_reads(a, b)
    assert any("document_type" in d for d in diffs)


def test_compare_reads_ignores_policy_type_text():
    """compare_extractions groups by family; GL vs CGL is the same family → no diff."""
    a = _make_ext(policies=[_gl_policy(policy_type="GL")])
    b = _make_ext(policies=[_gl_policy(policy_type="Commercial General Liability")])
    diffs = extractor._compare_reads(a, b)
    assert diffs == []


# ── normalize_policy_types ────────────────────────────────────────────────────

def test_normalize_gl_variants():
    for raw in ("GL", "CGL", "General Liability", "COMMERCIAL GENERAL LIABILITY"):
        data = {"policies": [{"policy_type": raw, "policy_number": "X"}]}
        result = extractor.normalize_policy_types(data)
        assert result["policies"][0]["policy_type"] == "Commercial General Liability", raw


def test_normalize_auto_variants():
    for raw in ("AUTO", "Automobile Liability", "Business Auto", "COMMERCIAL AUTO"):
        data = {"policies": [{"policy_type": raw, "policy_number": "X"}]}
        result = extractor.normalize_policy_types(data)
        assert result["policies"][0]["policy_type"] == "Commercial Auto", raw


def test_normalize_wc_variants():
    for raw in ("WC", "Workers Comp", "WORKERS COMPENSATION", "Workers' Compensation"):
        data = {"policies": [{"policy_type": raw, "policy_number": "X"}]}
        result = extractor.normalize_policy_types(data)
        assert result["policies"][0]["policy_type"] == "Workers Compensation", raw


def test_normalize_umbrella_variants():
    for raw in ("UMBRELLA", "Umbrella Liab", "umbrella liability"):
        data = {"policies": [{"policy_type": raw, "policy_number": "X"}]}
        result = extractor.normalize_policy_types(data)
        assert result["policies"][0]["policy_type"] == "Umbrella Liability", raw


def test_normalize_excess_variants():
    for raw in ("EXCESS", "Excess Liab", "EXCESS LIABILITY"):
        data = {"policies": [{"policy_type": raw, "policy_number": "X"}]}
        result = extractor.normalize_policy_types(data)
        assert result["policies"][0]["policy_type"] == "Excess Liability", raw


def test_normalize_unknown_kept_as_is():
    data = {"policies": [{"policy_type": "Professional Liability", "policy_number": "X"}]}
    result = extractor.normalize_policy_types(data)
    assert result["policies"][0]["policy_type"] == "Professional Liability"


def test_normalize_already_canonical_unchanged():
    data = {"policies": [{"policy_type": "Commercial General Liability", "policy_number": "X"}]}
    result = extractor.normalize_policy_types(data)
    assert result["policies"][0]["policy_type"] == "Commercial General Liability"


def test_normalize_no_policies_key():
    data = {"document_type": "COI"}
    result = extractor.normalize_policy_types(data)
    assert result == {"document_type": "COI"}


# ── extract_document: second read stored ─────────────────────────────────────

SAMPLE_DATA = {
    "document_type": "COI",
    "named_insured": "Acme LLC",
    "certificate_holder": "GC Inc",
    "contact_emails": [],
    "policies": [_gl_policy()],
    "confidence": 0.98,
}


@patch("extractor.ANTHROPIC_API_KEY", "test-key")
@patch("extractor.build_message_content", return_value=[{"type": "text", "text": "doc"}])
@patch("extractor._should_run_dedicated_policy_table_reader", return_value=False)
def test_extract_document_second_read_stored(mock_dedicated, mock_content):
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.return_value = _mock_response(SAMPLE_DATA)
        result = extractor.extract_document(Path("test.pdf"))

    assert "_second_read" in result
    assert isinstance(result["_ai_disagreements"], list)


@patch("extractor.ANTHROPIC_API_KEY", "test-key")
@patch("extractor.build_message_content", return_value=[{"type": "text", "text": "doc"}])
@patch("extractor._should_run_dedicated_policy_table_reader", return_value=False)
def test_extract_document_no_diffs_when_identical(mock_dedicated, mock_content):
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.return_value = _mock_response(SAMPLE_DATA)
        result = extractor.extract_document(Path("test.pdf"))

    assert result["_ai_disagreements"] == []


@patch("extractor.ANTHROPIC_API_KEY", "test-key")
@patch("extractor.build_message_content", return_value=[{"type": "text", "text": "doc"}])
@patch("extractor._should_run_dedicated_policy_table_reader", return_value=False)
def test_extract_document_diffs_when_reads_differ(mock_dedicated, mock_content):
    first = dict(SAMPLE_DATA)
    second = {**SAMPLE_DATA, "named_insured": "Different Corp"}
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.side_effect = [
            _mock_response(first),
            _mock_response(second),
        ]
        result = extractor.extract_document(Path("test.pdf"))

    assert any("named_insured" in d for d in result["_ai_disagreements"])


@patch("extractor.ANTHROPIC_API_KEY", "test-key")
@patch("extractor.build_message_content", return_value=[{"type": "text", "text": "doc"}])
@patch("extractor._should_run_dedicated_policy_table_reader", return_value=False)
def test_extract_document_second_read_failure_stored(mock_dedicated, mock_content):
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.side_effect = [
            _mock_response(SAMPLE_DATA),
            Exception("transient API error"),
        ]
        result = extractor.extract_document(Path("test.pdf"))

    assert result["_ai_disagreements"] == "Second read failed"
    assert result["_second_read"] is None
    # Primary extraction still returned
    assert result["document_type"] == "COI"


@patch("extractor.ANTHROPIC_API_KEY", "test-key")
@patch("extractor.build_message_content", return_value=[{"type": "text", "text": "doc"}])
@patch("extractor._should_run_dedicated_policy_table_reader", return_value=False)
def test_extract_document_primary_not_affected_by_second_failure(mock_dedicated, mock_content):
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.side_effect = [
            _mock_response(SAMPLE_DATA),
            Exception("API down"),
        ]
        result = extractor.extract_document(Path("test.pdf"))

    assert result["named_insured"] == "Acme LLC"
    assert result["confidence"] == 0.98


# ── review_gate: has_ai_disagreements ────────────────────────────────────────

def test_ai_disagreement_routes_to_pending_review():
    status, reason, proc = compute_review_status(
        confidence=0.99, is_possible_duplicate=False, has_ai_disagreements=True
    )
    assert status == REVIEW_STATUS_PENDING_REVIEW
    assert reason == REVIEW_REASON_AI_DISAGREEMENT
    assert proc == PROCESSING_STATUS_NEEDS_REVIEW


def test_ai_disagreement_overrides_auto_approval():
    """Even with confidence=1.0, disagreements force Pending Review."""
    status, reason, _ = compute_review_status(
        confidence=1.0, is_possible_duplicate=False, has_ai_disagreements=True
    )
    assert status == REVIEW_STATUS_PENDING_REVIEW
    assert reason == REVIEW_REASON_AI_DISAGREEMENT


def test_duplicate_trumps_ai_disagreement():
    """Possible Duplicate has higher precedence than AI Disagreement."""
    status, reason, proc = compute_review_status(
        confidence=0.99, is_possible_duplicate=True, has_ai_disagreements=True
    )
    assert reason == REVIEW_REASON_POSSIBLE_DUPLICATE
    assert proc == PROCESSING_STATUS_DUPLICATE


def test_no_disagreements_no_effect_on_auto_approve():
    status, reason, proc = compute_review_status(
        confidence=0.99, is_possible_duplicate=False, has_ai_disagreements=False
    )
    assert status == REVIEW_STATUS_AUTO_APPROVED
    assert proc == PROCESSING_STATUS_IMPORTED


def test_failed_second_read_string_does_not_trigger_pending():
    """'Second read failed' string is not a disagreement list; has_ai_disagreements=False."""
    # The caller determines has_ai_disagreements; a non-list value → False
    ai_disagrees = "Second read failed"
    has_ai_disagreements = isinstance(ai_disagrees, list) and len(ai_disagrees) > 0
    assert not has_ai_disagreements


# ── airtable_importer: build_fields with second_read / ai_disagreements ───────

def _base_data_clean():
    return {
        "document_type": "COI",
        "named_insured": "Acme LLC",
        "certificate_holder": "GC Inc",
        "contact_emails": ["agent@example.com"],
        "policies": [{"policy_number": "GL-1"}],
        "confidence": 0.99,
    }


def test_build_fields_second_read_written_to_field_id():
    data = _base_data_clean()
    second = {"document_type": "COI", "named_insured": "Acme LLC"}
    fields = build_fields("acme.json", data, json.dumps(data), second_read=second)
    assert SECOND_READ_JSON_FIELD_ID in fields
    parsed = json.loads(fields[SECOND_READ_JSON_FIELD_ID])
    assert parsed["document_type"] == "COI"


def test_build_fields_ai_disagrees_written_to_field_id():
    data = _base_data_clean()
    diffs = ["GL.wos: False vs True", "named_insured: 'Acme LLC' vs 'ACME LLC'"]
    fields = build_fields("acme.json", data, json.dumps(data), ai_disagreements=diffs)
    assert AI_DISAGREEMENTS_FIELD_ID in fields
    stored = fields[AI_DISAGREEMENTS_FIELD_ID]
    assert "GL.wos" in stored
    assert "named_insured" in stored


def test_build_fields_ai_disagrees_triggers_pending_review():
    data = _base_data_clean()
    diffs = ["GL.wos: False vs True"]
    fields = build_fields("acme.json", data, json.dumps(data), ai_disagreements=diffs)
    assert fields["Review Status"] == REVIEW_STATUS_PENDING_REVIEW
    assert fields["Review Reason"] == REVIEW_REASON_AI_DISAGREEMENT


def test_build_fields_empty_disagrees_does_not_trigger_pending():
    data = _base_data_clean()
    fields = build_fields("acme.json", data, json.dumps(data), ai_disagreements=[])
    assert fields["Review Status"] == REVIEW_STATUS_AUTO_APPROVED


def test_build_fields_second_read_failed_does_not_trigger_pending():
    data = _base_data_clean()
    fields = build_fields("acme.json", data, json.dumps(data),
                          ai_disagreements="Second read failed")
    assert fields["Review Status"] == REVIEW_STATUS_AUTO_APPROVED
    assert fields[AI_DISAGREEMENTS_FIELD_ID] == "Second read failed"


def test_build_fields_no_second_read_fields_absent_when_none():
    data = _base_data_clean()
    fields = build_fields("acme.json", data, json.dumps(data))
    assert SECOND_READ_JSON_FIELD_ID not in fields
    assert AI_DISAGREEMENTS_FIELD_ID not in fields


def test_build_fields_raw_json_does_not_contain_underscore_keys():
    """raw_json param should never contain _second_read/_ai_disagreements."""
    data = _base_data_clean()
    # Simulate caller passing clean_data (underscore keys already stripped)
    second = {"document_type": "COI"}
    diffs = ["GL.wos: False vs True"]
    clean_data = {k: v for k, v in data.items() if not k.startswith("_")}
    raw_json = json.dumps(clean_data, indent=2)
    fields = build_fields("acme.json", clean_data, raw_json,
                          second_read=second, ai_disagreements=diffs)
    raw = json.loads(fields["Raw JSON"])
    assert "_second_read" not in raw
    assert "_ai_disagreements" not in raw


# ── reextract_rtr._build_refresh_fields ──────────────────────────────────────

SAMPLE_EXTRACTION = {
    "document_type": "COI",
    "named_insured": "RTR Construction LLC",
    "certificate_holder": "Carolina Compliance Solutions",
    "contact_emails": ["rtr@example.com"],
    "policies": [
        {
            "policy_type": "GL",
            "policy_number": "GL-001",
            "carrier": "Acme Insurance",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1,000,000",
        }
    ],
    "confidence": 0.98,
}


def test_rtr_build_refresh_strips_underscore_keys_from_raw_json():
    data = dict(SAMPLE_EXTRACTION)
    data["_second_read"] = {"document_type": "COI"}
    data["_ai_disagreements"] = ["GL.wos: False vs True"]
    fields = reextract_rtr._build_refresh_fields(data)
    raw = json.loads(fields["Raw JSON"])
    assert "_second_read" not in raw
    assert "_ai_disagreements" not in raw


def test_rtr_build_refresh_writes_second_read_field():
    data = dict(SAMPLE_EXTRACTION)
    data["_second_read"] = {"document_type": "COI", "named_insured": "RTR Construction LLC"}
    data["_ai_disagreements"] = []
    fields = reextract_rtr._build_refresh_fields(data)
    assert reextract_rtr.SECOND_READ_JSON_FIELD_ID in fields
    parsed = json.loads(fields[reextract_rtr.SECOND_READ_JSON_FIELD_ID])
    assert parsed["document_type"] == "COI"


def test_rtr_build_refresh_writes_disagrees_field():
    data = dict(SAMPLE_EXTRACTION)
    data["_second_read"] = {"document_type": "COI"}
    data["_ai_disagreements"] = ["GL.wos: False vs True"]
    fields = reextract_rtr._build_refresh_fields(data)
    assert reextract_rtr.AI_DISAGREEMENTS_FIELD_ID in fields
    assert "GL.wos" in fields[reextract_rtr.AI_DISAGREEMENTS_FIELD_ID]


def test_rtr_build_refresh_no_underscore_data_omits_fields():
    fields = reextract_rtr._build_refresh_fields(dict(SAMPLE_EXTRACTION))
    assert reextract_rtr.SECOND_READ_JSON_FIELD_ID not in fields
    assert reextract_rtr.AI_DISAGREEMENTS_FIELD_ID not in fields


def test_rtr_build_refresh_second_read_failed_written():
    data = dict(SAMPLE_EXTRACTION)
    data["_second_read"] = None
    data["_ai_disagreements"] = "Second read failed"
    fields = reextract_rtr._build_refresh_fields(data)
    assert fields[reextract_rtr.AI_DISAGREEMENTS_FIELD_ID] == "Second read failed"
    assert reextract_rtr.SECOND_READ_JSON_FIELD_ID not in fields
