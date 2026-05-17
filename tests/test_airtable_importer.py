"""Unit tests for airtable_importer.build_fields confidence handling."""
import json

from airtable_importer import build_fields


def _base_data():
    return {
        "document_type": "COI",
        "named_insured": "Acme LLC",
        "certificate_holder": "GC Inc",
        "contact_emails": ["agent@example.com"],
        "policies": [{"policy_number": "GL-1"}],
    }


def test_build_fields_includes_confidence_when_present():
    data = _base_data()
    data["confidence"] = 0.92
    fields = build_fields("acme.json", data, json.dumps(data))
    assert fields["Confidence Score"] == 0.92


def test_build_fields_confidence_absent_writes_none():
    data = _base_data()
    fields = build_fields("acme.json", data, json.dumps(data))
    assert fields["Confidence Score"] is None


def test_build_fields_confidence_string_writes_none():
    data = _base_data()
    data["confidence"] = "very high"
    fields = build_fields("acme.json", data, json.dumps(data))
    assert fields["Confidence Score"] is None


def test_build_fields_confidence_int_coerced_to_float():
    data = _base_data()
    data["confidence"] = 1
    fields = build_fields("acme.json", data, json.dumps(data))
    assert fields["Confidence Score"] == 1.0
    assert isinstance(fields["Confidence Score"], float)


def test_build_fields_confidence_zero_kept():
    """0.0 is a valid confidence (used by triage path); must not be falsy-coerced."""
    data = _base_data()
    data["confidence"] = 0.0
    fields = build_fields("acme.json", data, json.dumps(data))
    assert fields["Confidence Score"] == 0.0


def test_build_fields_other_fields_unchanged():
    """Sanity: adding confidence didn't break the existing field mapping."""
    data = _base_data()
    data["confidence"] = 0.99  # high enough to keep Processing Status=Imported under 1E gate
    fields = build_fields("acme.json", data, json.dumps(data))
    assert fields["Source Filename"] == "acme.json"
    assert fields["Document Type"] == "COI"
    assert fields["Named Insured"] == "Acme LLC"
    assert fields["Processing Status"] == "Imported"
    assert "Raw JSON" in fields


# ── 1E: review gating in build_fields ────────────────────────────────────────


def test_build_fields_writes_auto_approved_for_high_confidence():
    data = _base_data()
    data["confidence"] = 0.99
    fields = build_fields("acme.json", data, json.dumps(data), is_possible_duplicate=False)
    assert fields["Processing Status"] == "Imported"
    assert fields["Review Status"] == "Auto-Approved"
    assert fields["Review Reason"] == "N/A"


def test_build_fields_writes_pending_review_for_low_confidence():
    data = _base_data()
    data["confidence"] = 0.5
    fields = build_fields("acme.json", data, json.dumps(data), is_possible_duplicate=False)
    assert fields["Processing Status"] == "Pending Review"
    assert fields["Review Status"] == "Pending Review"
    assert fields["Review Reason"] == "Low Confidence"


def test_build_fields_writes_pending_review_for_duplicate():
    data = _base_data()
    data["confidence"] = 0.99  # high — would auto-approve, but duplicate trumps
    fields = build_fields("acme.json", data, json.dumps(data), is_possible_duplicate=True)
    assert fields["Processing Status"] == "Pending Review"
    assert fields["Review Status"] == "Pending Review"
    assert fields["Review Reason"] == "Possible Duplicate"
