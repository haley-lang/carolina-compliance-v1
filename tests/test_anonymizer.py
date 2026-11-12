"""Tests for anonymizer.py."""
from anonymizer import anonymize, redact_names


class TestAnonymize:

    def test_known_name_redacted(self):
        result = anonymize("Acme GC sent the cert.", known_names=["Acme GC"])
        assert "Acme GC" not in result
        assert "[Contractor]" in result

    def test_multiple_names_redacted(self):
        result = anonymize("Call Smith's Roofing or Jones Brothers.", known_names=["Smith's Roofing", "Jones Brothers"])
        assert "Smith's Roofing" not in result
        assert "Jones Brothers" not in result

    def test_known_carrier_redacted(self):
        result = anonymize("Policy issued by State Farm.")
        assert "State Farm" not in result
        assert "[Carrier]" in result

    def test_multiple_carriers_redacted(self):
        result = anonymize("Travelers and Hartford both on file.")
        assert "Travelers" not in result
        assert "Hartford" not in result

    def test_policy_number_redacted(self):
        result = anonymize("Policy GL-1234567 expires next month.")
        assert "GL-1234567" not in result
        assert "[Policy#]" in result

    def test_street_address_redacted(self):
        result = anonymize("Send docs to 123 Main Street please.")
        assert "123 Main Street" not in result

    def test_city_state_zip_redacted(self):
        result = anonymize("Located in Charlotte, NC 28202.")
        assert "Charlotte, NC 28202" not in result

    def test_ein_redacted(self):
        result = anonymize("FEIN 12-3456789 on the cert.")
        assert "12-3456789" not in result
        assert "[EIN]" in result

    def test_email_redacted(self):
        result = anonymize("Send to vendor@example.com for review.")
        assert "vendor@example.com" not in result
        assert "[email]" in result

    def test_empty_string_returned_unchanged(self):
        assert anonymize("") == ""

    def test_none_safe(self):
        assert anonymize(None) is None

    def test_clean_text_unchanged(self):
        text = "A contractor got every certificate on file for the first time."
        result = anonymize(text)
        assert result == text

    def test_case_insensitive_name_match(self):
        result = anonymize("Contact ACME GC today.", known_names=["Acme GC"])
        assert "ACME GC" not in result

    def test_name_not_partial_matched(self):
        # "Acme" alone should not trigger redaction for known_name="Acme GC"
        result = anonymize("Acme is not the full name.", known_names=["Acme GC"])
        assert "Acme" in result  # partial match not redacted

    def test_blank_name_in_list_skipped(self):
        result = anonymize("Test text.", known_names=["", "  ", None])
        assert result == "Test text."


class TestRedactNames:
    def test_single_name(self):
        result = redact_names("RTR Construction called.", ["RTR Construction"])
        assert "RTR Construction" not in result
        assert "[Contractor]" in result

    def test_empty_names_list_no_change(self):
        text = "Some vendor called."
        assert redact_names(text, []) == text

    def test_none_text_returned(self):
        assert redact_names(None, ["name"]) is None
