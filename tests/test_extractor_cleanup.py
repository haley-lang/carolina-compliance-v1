from pathlib import Path
from unittest.mock import MagicMock, patch
import json
import os
import tempfile

from extractor import (
    apply_simple_document_classification,
    drop_empty_policies,
    _is_zero_or_blank_limits,
    _is_phantom_by_limits,
    _is_phantom_by_duplicate_number,
    _expand_bundle,
    _process_single_file,
)


def test_coi_with_cancellation_wording_stays_coi():
    data = {
        "document_type": "COI",
        "named_insured": "Griffin Masonry, Inc",
        "description_of_operations": "Includes 30 day notice of cancellation, subject to policy terms.",
        "policies": [],
    }
    assert apply_simple_document_classification(data, Path("COI_forms_cert07.pdf"))["document_type"] == "COI"


def test_model_cancellation_type_is_kept():
    data = {"document_type": "cancellation_notice", "policies": []}
    assert apply_simple_document_classification(data, Path("x.pdf"))["document_type"] == "cancellation_notice"


def test_reinstatement_type_from_model():
    data = {"document_type": "reinstatement", "policies": []}
    assert apply_simple_document_classification(data, Path("x.pdf"))["document_type"] == "reinstatement"


def test_drop_empty_policies_keeps_partial_rows():
    # GL: number only, no carrier, no limits → dropped by Condition A (phantom-by-limits)
    # UMBRELLA: all blank → dropped by Pass 1
    # AUTO: has carrier → kept even without limits
    # WC: blank number and carrier → dropped by Pass 1
    data = {"policies": [
        {"policy_type": "GL", "policy_number": "ABC1", "carrier": None},
        {"policy_type": "UMBRELLA LIAB", "policy_number": None, "carrier": None,
         "effective_date": None, "expiration_date": None, "coverage_limits": None},
        {"policy_type": "AUTO", "policy_number": None, "carrier": "Erie", "effective_date": None},
        {"policy_type": "WC", "policy_number": " ", "carrier": ""},
    ]}
    out = drop_empty_policies(data)
    assert [p["policy_type"] for p in out["policies"]] == ["AUTO"]


# ── _is_zero_or_blank_limits ──────────────────────────────────────────────────

def test_zero_limits_null():
    assert _is_zero_or_blank_limits(None) is True

def test_zero_limits_empty_string():
    assert _is_zero_or_blank_limits("") is True

def test_zero_limits_dollar_zero():
    assert _is_zero_or_blank_limits("$0") is True

def test_zero_limits_dollar_zero_slash():
    assert _is_zero_or_blank_limits("$0/$0") is True

def test_zero_limits_zero_each_agg():
    assert _is_zero_or_blank_limits("$0 EA OCC / $0 AGG") is True

def test_zero_limits_no_dollar_amounts():
    assert _is_zero_or_blank_limits("See policy") is True

def test_zero_limits_real_amount():
    assert _is_zero_or_blank_limits("$1,000,000") is False

def test_zero_limits_mixed_zero_and_real():
    assert _is_zero_or_blank_limits("$0 / $1,000,000") is False


# ── _is_phantom_by_limits (Condition A) ──────────────────────────────────────

def test_phantom_by_limits_cert06_gl_row():
    """cert06 GL phantom: WC policy number + $0 limits, no carrier, no boxes."""
    p = {
        "policy_type": "Commercial General Liability",
        "policy_number": "WC-9876543",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": None,
        "coverage_limits": "$0",
        "additional_insured_checked": False,
        "waiver_of_subrogation_checked": False,
        "primary_noncontributory_checked": False,
    }
    assert _is_phantom_by_limits(p) is True

def test_phantom_by_limits_kept_when_carrier_present():
    p = {
        "policy_type": "Commercial General Liability",
        "policy_number": "WC-9876543",
        "carrier": "SFM",
        "coverage_limits": "$0",
        "additional_insured_checked": False,
        "waiver_of_subrogation_checked": False,
        "primary_noncontributory_checked": False,
    }
    assert _is_phantom_by_limits(p) is False

def test_phantom_by_limits_kept_when_checked_box():
    p = {
        "policy_type": "Commercial General Liability",
        "policy_number": "WC-9876543",
        "carrier": None,
        "coverage_limits": "$0",
        "additional_insured_checked": True,
        "waiver_of_subrogation_checked": False,
        "primary_noncontributory_checked": False,
    }
    assert _is_phantom_by_limits(p) is False

def test_phantom_by_limits_kept_when_real_limits():
    p = {
        "policy_type": "Commercial General Liability",
        "policy_number": "GL-111",
        "carrier": None,
        "coverage_limits": "$1,000,000",
        "additional_insured_checked": False,
        "waiver_of_subrogation_checked": False,
        "primary_noncontributory_checked": False,
    }
    assert _is_phantom_by_limits(p) is False


# ── _is_phantom_by_duplicate_number (Condition B) ────────────────────────────

def _wc_row():
    return {
        "policy_type": "Workers Compensation",
        "policy_number": "WC-9876543",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": "SFM",
        "coverage_limits": "$500,000",
    }

def _gl_phantom():
    return {
        "policy_type": "Commercial General Liability",
        "policy_number": "WC-9876543",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": None,
        "coverage_limits": "$0",
        "additional_insured_checked": True,
    }

def test_phantom_by_dupe_number_detected():
    gl = _gl_phantom()
    wc = _wc_row()
    assert _is_phantom_by_duplicate_number(gl, [gl, wc]) is True

def test_phantom_by_dupe_number_real_limits_kept():
    """If the phantom has real limits, Condition B does not apply."""
    p = {**_gl_phantom(), "coverage_limits": "$1,000,000"}
    wc = _wc_row()
    assert _is_phantom_by_duplicate_number(p, [p, wc]) is False

def test_phantom_by_dupe_number_same_type_not_phantom():
    """Two rows of the same policy_type are not a type-mismatch."""
    gl1 = {**_gl_phantom(), "policy_number": "GL-111"}
    gl2 = {**_gl_phantom(), "policy_number": "GL-111"}
    assert _is_phantom_by_duplicate_number(gl1, [gl1, gl2]) is False

def test_phantom_by_dupe_number_no_match_when_dates_differ():
    gl = {**_gl_phantom(), "expiration_date": "2099-01-01"}
    wc = _wc_row()
    assert _is_phantom_by_duplicate_number(gl, [gl, wc]) is False

def test_phantom_by_dupe_number_no_match_blank_number():
    gl = {**_gl_phantom(), "policy_number": ""}
    wc = _wc_row()
    assert _is_phantom_by_duplicate_number(gl, [gl, wc]) is False


# ── drop_empty_policies end-to-end ───────────────────────────────────────────

def test_drop_phantom_cert06_shape():
    """Full cert06 shape: WC real row + GL phantom (copies WC number+dates+$0)."""
    wc = {
        "policy_type": "Workers Compensation",
        "policy_number": "WC-9876543",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": "SFM",
        "coverage_limits": "$500,000",
        "additional_insured_checked": False,
        "waiver_of_subrogation_checked": False,
        "primary_noncontributory_checked": False,
    }
    gl_phantom = {
        "policy_type": "Commercial General Liability",
        "policy_number": "WC-9876543",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": None,
        "coverage_limits": "$0",
        "additional_insured_checked": False,
        "waiver_of_subrogation_checked": False,
        "primary_noncontributory_checked": False,
    }
    out = drop_empty_policies({"policies": [wc, gl_phantom]})
    types = [p["policy_type"] for p in out["policies"]]
    assert types == ["Workers Compensation"]

def test_drop_phantom_condition_b_with_checked_box():
    """Condition B still fires even when the phantom row has a checked box."""
    wc = {
        "policy_type": "Workers Compensation",
        "policy_number": "WC-9876543",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": "SFM",
        "coverage_limits": "$500,000",
    }
    gl_phantom = {
        "policy_type": "Commercial General Liability",
        "policy_number": "WC-9876543",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": None,
        "coverage_limits": "$0",
        "additional_insured_checked": True,
    }
    out = drop_empty_policies({"policies": [wc, gl_phantom]})
    types = [p["policy_type"] for p in out["policies"]]
    assert types == ["Workers Compensation"]

def test_drop_phantom_real_gl_and_wc_both_kept():
    """Two real rows with different types and both having real data are kept."""
    wc = {
        "policy_type": "Workers Compensation",
        "policy_number": "WC-001",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": "SFM",
        "coverage_limits": "$500,000",
    }
    gl = {
        "policy_type": "Commercial General Liability",
        "policy_number": "GL-999",
        "effective_date": "2024-01-01",
        "expiration_date": "2025-01-01",
        "carrier": "Acuity",
        "coverage_limits": "$1,000,000",
    }
    out = drop_empty_policies({"policies": [gl, wc]})
    assert len(out["policies"]) == 2

def test_drop_phantom_no_policies_unchanged():
    data = {"policies": []}
    out = drop_empty_policies(data)
    assert out["policies"] == []

def test_drop_phantom_non_list_policies_unchanged():
    data = {"policies": None}
    out = drop_empty_policies(data)
    assert out["policies"] is None


# ── _expand_bundle: single-page PDF fallback ──────────────────────────────────

def test_expand_bundle_single_page_pdf_returns_original():
    """When split_pdf returns [] (single-page PDF), _expand_bundle must return [file_path],
    not an empty list.  This was the root bug for RTR_COI_page47_resend.pdf."""
    fake_pdf = Path("/tmp/RTR_COI_page47_resend.pdf")
    with patch("extractor.pdf_bundle.split_pdf", return_value=[]) as mock_split:
        result = _expand_bundle(fake_pdf)
    mock_split.assert_called_once()
    assert result == [fake_pdf], "single-page PDF must fall back to [file_path], not []"


def test_expand_bundle_non_pdf_skips_split():
    """Non-PDF files bypass split_pdf entirely and return [file_path]."""
    fake_img = Path("/tmp/cert.png")
    with patch("extractor.pdf_bundle.split_pdf") as mock_split:
        result = _expand_bundle(fake_img)
    mock_split.assert_not_called()
    assert result == [fake_img]


# ── _process_single_file: image triage fallback ───────────────────────────────

def _make_png(tmp_dir: str, name: str = "cert.png") -> Path:
    p = Path(tmp_dir) / name
    p.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 16)  # minimal PNG header
    return p


def test_png_happy_path_saves_extraction():
    """When extract_document succeeds for a PNG, the result is saved normally."""
    good_data = {
        "document_type": "COI",
        "named_insured": "Rocha LLC",
        "certificate_holder": None,
        "certificate_date": "2026-01-01",
        "contact_emails": [],
        "description_of_operations": None,
        "policies": [{"policy_type": "GL", "policy_number": "GL-001",
                       "carrier": "Acuity", "coverage_limits": "$1,000,000",
                       "effective_date": "2026-01-01", "expiration_date": "2027-01-01"}],
        "confidence": 0.9,
    }
    with tempfile.TemporaryDirectory() as tmp:
        png = _make_png(tmp)
        with (
            patch("extractor.EXTRACT_DIR", Path(tmp)),
            patch("extractor.extract_document", return_value=good_data),
            patch("extractor.normalize_policy_types", side_effect=lambda d: d),
            patch("extractor.normalize_policy_dates", side_effect=lambda d: d),
            patch("extractor.apply_simple_document_classification", side_effect=lambda d, _f: d),
            patch("extractor.drop_empty_policies", side_effect=lambda d: d),
        ):
            result = _process_single_file(png)

        json_path = Path(tmp) / (png.stem + ".json")
        assert json_path.exists(), "JSON must be written for a successful PNG extraction"
        saved = json.loads(json_path.read_text())
        assert saved["named_insured"] == "Rocha LLC"
        assert result["confidence"] == 0.9


def test_png_extraction_failure_saves_triage_json():
    """When extract_document raises for a PNG, a triage JSON is saved with confidence=0.
    This ensures the RECONCILE check passes and the importer creates a record.
    Root cause of the Rocha / recqAwPFbvAluTifC missed intake."""
    with tempfile.TemporaryDirectory() as tmp:
        png = _make_png(tmp, "rocha_coi.png")
        with (
            patch("extractor.EXTRACT_DIR", Path(tmp)),
            patch("extractor.extract_document",
                  side_effect=RuntimeError("Claude API timeout")),
        ):
            result = _process_single_file(png)

        json_path = Path(tmp) / "rocha_coi.json"
        assert json_path.exists(), "Triage JSON must be written even when extraction fails"
        saved = json.loads(json_path.read_text())
        assert saved["confidence"] == 0.0
        assert "_triage_reason" in saved
        assert "Image extraction failed" in saved["_triage_reason"]
        assert result["confidence"] == 0.0


def test_jpg_extraction_failure_saves_triage_json():
    """Same triage fallback applies to JPG files."""
    with tempfile.TemporaryDirectory() as tmp:
        jpg = Path(tmp) / "cert.jpg"
        jpg.write_bytes(b"\xff\xd8\xff" + b"\x00" * 16)
        with (
            patch("extractor.EXTRACT_DIR", Path(tmp)),
            patch("extractor.extract_document",
                  side_effect=ValueError("No JSON in response")),
        ):
            result = _process_single_file(jpg)

        json_path = Path(tmp) / "cert.json"
        assert json_path.exists()
        saved = json.loads(json_path.read_text())
        assert saved["confidence"] == 0.0


def test_pdf_extraction_failure_still_propagates():
    """PDF extraction errors are NOT caught by the image triage path — they
    propagate to the batch loop as before."""
    with tempfile.TemporaryDirectory() as tmp:
        pdf = Path(tmp) / "cert.pdf"
        pdf.write_bytes(b"%PDF-1.4\n")
        with (
            patch("extractor.EXTRACT_DIR", Path(tmp)),
            patch("extractor.pdf_bundle.pdf_has_text_layer", return_value=True),
            patch("extractor.classify_pdf_pages", return_value={1: "acord_25"}),
            patch("extractor.extract_document",
                  side_effect=RuntimeError("API error")),
        ):
            try:
                _process_single_file(pdf)
                assert False, "Expected RuntimeError to propagate for PDFs"
            except RuntimeError:
                pass  # correct — PDF errors are not swallowed
