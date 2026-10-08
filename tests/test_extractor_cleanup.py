from pathlib import Path

from extractor import apply_simple_document_classification, drop_empty_policies


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
    data = {"policies": [
        {"policy_type": "GL", "policy_number": "ABC1", "carrier": None},
        {"policy_type": "UMBRELLA LIAB", "policy_number": None, "carrier": None,
         "effective_date": None, "expiration_date": None, "coverage_limits": None},
        {"policy_type": "AUTO", "policy_number": None, "carrier": "Erie", "effective_date": None},
        {"policy_type": "WC", "policy_number": " ", "carrier": ""},
    ]}
    out = drop_empty_policies(data)
    assert [p["policy_type"] for p in out["policies"]] == ["GL", "AUTO"]
