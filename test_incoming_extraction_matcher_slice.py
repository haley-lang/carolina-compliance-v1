import unittest
from unittest.mock import MagicMock

from incoming_extraction_matcher_slice import (
    MATCH_STATUS_ERROR,
    MATCH_STATUS_MATCHED,
    MATCH_STATUS_PENDING,
    REASON_AMBIGUOUS_CLIENT,
    REASON_AMBIGUOUS_REQUEST_CONTEXT,
    REASON_AMBIGUOUS_VENDOR,
    REASON_INVALID_REFERENCED_ID,
    REASON_MISSING_REQUIRED_FIELDS,
    REASON_NO_CANDIDATE_FOUND,
    RULE_C1_CERTIFICATE_HOLDER_CLIENT_EXACT,
    RULE_R1_SINGLE_OPEN_REQUEST,
    RULE_V2_NAMED_INSURED_VENDOR_EXACT,
    MatchEvaluation,
    apply_incoming_extraction_match_update,
    build_match_decision_summary,
    build_incoming_extraction_patch,
    evaluate_vendor_match,
    normalize_match_text,
)


class TestIncomingExtractionMatcherSlice(unittest.TestCase):
    def test_normalize_match_text_is_deterministic(self):
        self.assertEqual(normalize_match_text("  Acme   LLC  "), "acme llc")
        self.assertEqual(normalize_match_text("ACME\nLLC"), "acme llc")
        self.assertEqual(normalize_match_text(None), "")

    def test_matched_has_blank_reason_code(self):
        extraction_fields = {"Named Insured": "Acme LLC"}
        vendors = [{"id": "ven_1", "fields": {"Vendor Name": "  acme llc  "}}]

        result = evaluate_vendor_match(extraction_fields, vendors)

        self.assertEqual(result.match_status, MATCH_STATUS_MATCHED)
        self.assertIsNone(result.match_reason_code)
        self.assertEqual(result.matched_vendor_id, "ven_1")
        self.assertEqual(result.applied_rule_id, RULE_V2_NAMED_INSURED_VENDOR_EXACT)

    def test_missing_required_fields_maps_to_pending_match(self):
        extraction_fields = {"Named Insured": "   "}
        result = evaluate_vendor_match(extraction_fields, [])

        self.assertEqual(result.match_status, MATCH_STATUS_PENDING)
        self.assertEqual(result.match_reason_code, REASON_MISSING_REQUIRED_FIELDS)
        self.assertIsNone(result.matched_vendor_id)

    def test_corrupt_vendor_reference_maps_to_error(self):
        extraction_fields = {"Named Insured": "Acme LLC"}
        vendors = [{"id": "ven_1", "fields": "not-a-dict"}]

        result = evaluate_vendor_match(extraction_fields, vendors)

        self.assertEqual(result.match_status, MATCH_STATUS_ERROR)
        self.assertEqual(result.match_reason_code, REASON_INVALID_REFERENCED_ID)
        self.assertIsNone(result.matched_vendor_id)

    def test_technical_failure_maps_to_error(self):
        extraction_fields = {"Named Insured": "Acme LLC"}

        result = evaluate_vendor_match(extraction_fields, None)

        self.assertEqual(result.match_status, MATCH_STATUS_ERROR)
        self.assertEqual(result.match_reason_code, REASON_INVALID_REFERENCED_ID)
        self.assertIsNone(result.matched_vendor_id)

    def test_no_match_maps_to_pending(self):
        extraction_fields = {"Named Insured": "Acme LLC"}
        vendors = [{"id": "ven_2", "fields": {"Vendor Name": "Beta Inc"}}]

        result = evaluate_vendor_match(extraction_fields, vendors)

        self.assertEqual(result.match_status, MATCH_STATUS_PENDING)
        self.assertEqual(result.match_reason_code, REASON_NO_CANDIDATE_FOUND)
        self.assertIsNone(result.matched_vendor_id)

    def test_ambiguous_match_maps_to_pending(self):
        extraction_fields = {"Named Insured": "Acme LLC"}
        vendors = [
            {"id": "ven_1", "fields": {"Vendor Name": "Acme LLC"}},
            {"id": "ven_2", "fields": {"Vendor Name": "ACME LLC"}},
        ]

        result = evaluate_vendor_match(extraction_fields, vendors)

        self.assertEqual(result.match_status, MATCH_STATUS_PENDING)
        self.assertEqual(result.match_reason_code, REASON_AMBIGUOUS_VENDOR)
        self.assertIsNone(result.matched_vendor_id)

    def test_build_match_decision_summary_is_deterministic(self):
        evaluation = MatchEvaluation(
            match_status=MATCH_STATUS_PENDING,
            match_reason_code=REASON_MISSING_REQUIRED_FIELDS,
            matched_vendor_id=None,
            matched_client_id="cli_1",
            matched_request_id="req_2",
            applied_rule_id="rule_3",
        )

        summary = build_match_decision_summary(evaluation)
        self.assertEqual(
            summary,
            "status=Pending Match; reason=MISSING_REQUIRED_FIELDS; vendor=; client=cli_1; request=req_2; rule=rule_3",
        )

    def test_alias_exact_match_works_without_heuristics(self):
        extraction_fields = {"Named Insured": "Acme Builders"}
        vendors = [
            {
                "id": "ven_1",
                "fields": {
                    "Vendor Name": "Acme Construction LLC",
                    "Aliases": "Acme Builders, Acme Build Co",
                },
            }
        ]

        result = evaluate_vendor_match(extraction_fields, vendors)
        self.assertEqual(result.match_status, MATCH_STATUS_MATCHED)
        self.assertEqual(result.matched_vendor_id, "ven_1")
        self.assertEqual(result.applied_rule_id, RULE_V2_NAMED_INSURED_VENDOR_EXACT)

    def test_client_resolution_applies_only_after_unique_vendor(self):
        extraction_fields = {
            "Named Insured": "Acme LLC",
            "Certificate Holder": "City of Raleigh",
        }
        vendors = [{"id": "ven_1", "fields": {"Vendor Name": "Acme LLC"}}]
        clients = [
            {
                "id": "cli_1",
                "fields": {
                    "Certificate Holder": "City of Raleigh",
                    "Vendor": ["ven_1"],
                },
            }
        ]

        result = evaluate_vendor_match(
            extraction_fields,
            vendors,
            client_records=clients,
        )

        self.assertEqual(result.match_status, MATCH_STATUS_MATCHED)
        self.assertEqual(result.matched_vendor_id, "ven_1")
        self.assertEqual(result.matched_client_id, "cli_1")
        self.assertEqual(result.applied_rule_id, RULE_C1_CERTIFICATE_HOLDER_CLIENT_EXACT)

    def test_client_resolution_requires_deterministic_single_result(self):
        extraction_fields = {
            "Named Insured": "Acme LLC",
            "Certificate Holder": "City of Raleigh",
        }
        vendors = [{"id": "ven_1", "fields": {"Vendor Name": "Acme LLC"}}]
        clients = [
            {
                "id": "cli_1",
                "fields": {
                    "Certificate Holder": "City of Raleigh",
                    "Vendor": ["ven_1"],
                },
            },
            {
                "id": "cli_2",
                "fields": {
                    "Certificate Holder": "City of Raleigh",
                    "Vendor": ["ven_1"],
                },
            },
        ]

        result = evaluate_vendor_match(
            extraction_fields,
            vendors,
            client_records=clients,
        )

        self.assertEqual(result.match_status, MATCH_STATUS_PENDING)
        self.assertEqual(result.match_reason_code, REASON_AMBIGUOUS_CLIENT)
        self.assertEqual(result.matched_vendor_id, "ven_1")
        self.assertIsNone(result.matched_client_id)

    def test_request_resolution_auto_links_only_single_open_request(self):
        extraction_fields = {
            "Named Insured": "Acme LLC",
            "Certificate Holder": "City of Raleigh",
        }
        vendors = [{"id": "ven_1", "fields": {"Vendor Name": "Acme LLC"}}]
        clients = [
            {
                "id": "cli_1",
                "fields": {
                    "Certificate Holder": "City of Raleigh",
                    "Vendor": ["ven_1"],
                },
            }
        ]
        requests = [
            {
                "id": "req_open",
                "fields": {
                    "Vendor": ["ven_1"],
                    "Client": ["cli_1"],
                    "Status": "Open",
                },
            },
            {
                "id": "req_closed",
                "fields": {
                    "Vendor": ["ven_1"],
                    "Client": ["cli_1"],
                    "Status": "Closed",
                },
            },
        ]

        result = evaluate_vendor_match(
            extraction_fields,
            vendors,
            client_records=clients,
            request_records=requests,
        )

        self.assertEqual(result.match_status, MATCH_STATUS_MATCHED)
        self.assertEqual(result.matched_vendor_id, "ven_1")
        self.assertEqual(result.matched_client_id, "cli_1")
        self.assertEqual(result.matched_request_id, "req_open")
        self.assertEqual(result.applied_rule_id, RULE_R1_SINGLE_OPEN_REQUEST)

    def test_request_resolution_stops_on_ambiguous_open_requests(self):
        extraction_fields = {
            "Named Insured": "Acme LLC",
            "Certificate Holder": "City of Raleigh",
        }
        vendors = [{"id": "ven_1", "fields": {"Vendor Name": "Acme LLC"}}]
        clients = [
            {
                "id": "cli_1",
                "fields": {
                    "Certificate Holder": "City of Raleigh",
                    "Vendor": ["ven_1"],
                },
            }
        ]
        requests = [
            {
                "id": "req_1",
                "fields": {
                    "Vendor": ["ven_1"],
                    "Client": ["cli_1"],
                    "Status": "Open",
                },
            },
            {
                "id": "req_2",
                "fields": {
                    "Vendor": ["ven_1"],
                    "Client": ["cli_1"],
                    "Request Status": "Open",
                },
            },
        ]

        result = evaluate_vendor_match(
            extraction_fields,
            vendors,
            client_records=clients,
            request_records=requests,
        )

        self.assertEqual(result.match_status, MATCH_STATUS_PENDING)
        self.assertEqual(result.match_reason_code, REASON_AMBIGUOUS_REQUEST_CONTEXT)
        self.assertEqual(result.matched_vendor_id, "ven_1")
        self.assertEqual(result.matched_client_id, "cli_1")
        self.assertIsNone(result.matched_request_id)

    def test_build_patch_is_idempotent_for_equivalent_blank_reason(self):
        existing_fields = {
            "Match Status": MATCH_STATUS_MATCHED,
            "Match Reason Code": "",
            "Match Decision Summary": "status=Matched; reason=; vendor=ven_1; client=; request=; rule=",
            "Matched Vendor": ["ven_1"],
            "Matched Client": [],
            "Matched Client Request": [],
        }
        evaluation = MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id="ven_1",
            matched_client_id=None,
            matched_request_id=None,
            applied_rule_id=None,
        )

        patch = build_incoming_extraction_patch(existing_fields, evaluation)
        self.assertEqual(patch, {})

    def test_apply_update_writes_only_when_changed(self):
        table = MagicMock()
        existing_fields = {
            "Match Status": MATCH_STATUS_PENDING,
            "Match Reason Code": REASON_NO_CANDIDATE_FOUND,
            "Match Decision Summary": "status=Pending Match; reason=NO_CANDIDATE_FOUND; vendor=; client=; request=; rule=",
            "Matched Vendor": [],
            "Matched Client": [],
            "Matched Client Request": [],
        }
        evaluation = MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id="ven_1",
            matched_client_id="cli_1",
            matched_request_id="req_1",
            applied_rule_id="rule_1",
        )

        changed = apply_incoming_extraction_match_update(
            incoming_extractions_table=table,
            record_id="rec_123",
            existing_fields=existing_fields,
            evaluation=evaluation,
        )

        self.assertTrue(changed)
        table.update.assert_called_once_with(
            "rec_123",
            {
                "Match Status": MATCH_STATUS_MATCHED,
                "Match Reason Code": None,
                "Match Decision Summary": "status=Matched; reason=; vendor=ven_1; client=cli_1; request=req_1; rule=rule_1",
                "Matched Vendor": ["ven_1"],
                "Matched Client": ["cli_1"],
                "Matched Client Request": ["req_1"],
            },
        )

    def test_apply_update_is_noop_when_already_in_target_state(self):
        table = MagicMock()
        existing_fields = {
            "Match Status": MATCH_STATUS_MATCHED,
            "Match Reason Code": None,
            "Match Decision Summary": "status=Matched; reason=; vendor=ven_1; client=cli_1; request=req_1; rule=rule_1",
            "Matched Vendor": ["ven_1"],
            "Matched Client": ["cli_1"],
            "Matched Client Request": ["req_1"],
        }
        evaluation = MatchEvaluation(
            match_status=MATCH_STATUS_MATCHED,
            match_reason_code=None,
            matched_vendor_id="ven_1",
            matched_client_id="cli_1",
            matched_request_id="req_1",
            applied_rule_id="rule_1",
        )

        changed = apply_incoming_extraction_match_update(
            incoming_extractions_table=table,
            record_id="rec_123",
            existing_fields=existing_fields,
            evaluation=evaluation,
        )

        self.assertFalse(changed)
        table.update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
