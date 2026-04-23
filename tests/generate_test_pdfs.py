"""
Synthetic ACORD 25-style test PDF generator using reportlab.

This module generates minimal PDFs for edge case testing of a COI extraction pipeline.
DO NOT use real client data.
"""

import os
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, gray
from reportlab.pdfgen import canvas
from reportlab.lib import colors


class AcordTestPDFGenerator:
    """Generate synthetic ACORD 25 and related test PDFs for extraction pipeline testing."""

    LETTER_WIDTH, LETTER_HEIGHT = letter
    MARGIN = 0.5 * inch
    TEXT_COLOR = colors.black
    LIGHT_TEXT_COLOR = HexColor("#CCCCCC")
    HEADER_Y = LETTER_HEIGHT - 0.75 * inch

    def __init__(self):
        """Initialize the generator."""
        pass

    def _draw_standard_header(self, c, form_version="ACORD 25 (2016/03)"):
        """Draw the standard ACORD header."""
        c.setFont("Helvetica-Bold", 14)
        c.drawString(self.MARGIN, self.HEADER_Y, "ACORD CERTIFICATE OF LIABILITY INSURANCE")
        c.setFont("Helvetica", 9)
        c.drawString(self.MARGIN, self.HEADER_Y - 0.2 * inch, f"Form: {form_version}")

    def _draw_standard_form_content(
        self,
        c,
        named_insured="Test Construction LLC",
        certificate_holder="General Contractor Inc",
        certificate_date="01/15/2025",
        text_color=None,
        include_policy_section=True,
        include_insurer_name=True,
        include_certificate_holder=True,
        include_named_insured=True,
        blank_coverage=False,
        gl_limit="$2,000,000",
    ):
        """Draw standard ACORD 25 form content."""
        if text_color is None:
            text_color = self.TEXT_COLOR

        y = self.HEADER_Y - 0.6 * inch
        c.setFont("Helvetica-Bold", 10)
        c.setFillColor(text_color)

        # Named Insured Section
        if include_named_insured:
            c.drawString(self.MARGIN, y, "NAMED INSURED:")
            c.setFont("Helvetica", 10)
            c.drawString(self.MARGIN + 1.2 * inch, y, named_insured)
            y -= 0.3 * inch

        # Certificate Holder Section
        if include_certificate_holder:
            c.setFont("Helvetica-Bold", 10)
            c.drawString(self.MARGIN, y, "CERTIFICATE HOLDER:")
            c.setFont("Helvetica", 10)
            c.drawString(self.MARGIN + 1.2 * inch, y, certificate_holder)
            y -= 0.3 * inch

        # Certificate Date
        c.setFont("Helvetica-Bold", 10)
        c.drawString(self.MARGIN, y, "CERTIFICATE DATE:")
        c.setFont("Helvetica", 10)
        c.drawString(self.MARGIN + 1.2 * inch, y, certificate_date)
        y -= 0.5 * inch

        # Insurance Companies Section
        if include_insurer_name:
            c.setFont("Helvetica-Bold", 10)
            c.drawString(self.MARGIN, y, "INSURANCE COMPANY INFORMATION")
            y -= 0.25 * inch
            c.setFont("Helvetica-Bold", 9)
            c.drawString(self.MARGIN, y, "Insurer Name")
            c.drawString(self.MARGIN + 2.0 * inch, y, "Policy Number")
            c.drawString(self.MARGIN + 3.5 * inch, y, "Inception Date")
            c.drawString(self.MARGIN + 4.5 * inch, y, "Expiration Date")
            y -= 0.2 * inch

            # Draw table lines
            c.setLineWidth(0.5)
            c.line(self.MARGIN, y, self.LETTER_WIDTH - self.MARGIN, y)

            # Policy rows
            if include_policy_section:
                policies = [
                    ("ABC Insurance Co.", "GL-12345", "01/15/2024", "01/15/2025", "General Liability"),
                    ("XYZ Insurance Co.", "WC-67890", "01/15/2024", "01/15/2025", "Workers Comp"),
                    ("DEF Insurance Co.", "AU-11111", "01/15/2024", "01/15/2025", "Commercial Auto"),
                ]

                for insurer, policy_num, inception, expiration, policy_type in policies:
                    y -= 0.25 * inch
                    c.setFont("Helvetica", 9)
                    c.drawString(self.MARGIN, y, insurer)
                    c.drawString(self.MARGIN + 2.0 * inch, y, policy_num)
                    c.drawString(self.MARGIN + 3.5 * inch, y, inception)
                    c.drawString(self.MARGIN + 4.5 * inch, y, expiration)
                    y -= 0.2 * inch
                    c.setFont("Helvetica-Oblique", 8)
                    c.drawString(self.MARGIN + 0.1 * inch, y, policy_type)

        # Coverage Section
        y -= 0.4 * inch
        c.setFont("Helvetica-Bold", 10)
        c.drawString(self.MARGIN, y, "COVERAGE PROVIDED")
        y -= 0.25 * inch

        c.setFont("Helvetica-Bold", 9)
        c.drawString(self.MARGIN, y, "Coverage Type")
        c.drawString(self.MARGIN + 2.5 * inch, y, "Each Occurrence")
        c.drawString(self.MARGIN + 4.5 * inch, y, "Aggregate")
        y -= 0.2 * inch

        c.setLineWidth(0.5)
        c.line(self.MARGIN, y, self.LETTER_WIDTH - self.MARGIN, y)

        # Coverage rows
        coverage_data = [
            ("General Liability", gl_limit if not blank_coverage else "N/A", "$2,000,000"),
            ("Workers Compensation", "$1,000,000" if not blank_coverage else "N/A", "$1,000,000"),
            ("Commercial Auto", "$1,000,000" if not blank_coverage else "N/A", "$1,000,000"),
        ]

        for coverage_type, each_occurrence, aggregate in coverage_data:
            y -= 0.25 * inch
            c.setFont("Helvetica", 9)
            c.drawString(self.MARGIN, y, coverage_type)
            c.drawString(self.MARGIN + 2.5 * inch, y, each_occurrence)
            c.drawString(self.MARGIN + 4.5 * inch, y, aggregate)

    def generate_standard_acord25(self, output_path):
        """Generate a clean, standard ACORD 25 (2016/03) certificate."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("ACORD 25 Certificate of Liability Insurance")

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(c)

        c.showPage()
        c.save()

    def generate_low_resolution(self, output_path):
        """Generate same content but with low resolution simulation."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("ACORD 25 Low Resolution")

        # Add noise-like gray rectangles
        c.setFillColor(HexColor("#EEEEEE"))
        for i in range(0, int(self.LETTER_WIDTH), 15):
            for j in range(0, int(self.LETTER_HEIGHT), 15):
                if (i + j) % 30 == 0:
                    c.rect(i, j, 8, 8, fill=1, stroke=0)

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        # Use smaller fonts for low resolution effect
        c.setFont("Helvetica", 7)
        self._draw_standard_form_content(c)

        c.showPage()
        c.save()

    def generate_rotated(self, output_path, angle=5):
        """Generate standard cert but rotated at specified angle."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("ACORD 25 Rotated")

        c.rotate(angle)
        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(c)

        c.showPage()
        c.save()

    def generate_partial_cutoff(self, output_path):
        """Generate standard cert but bottom 20% is cut off (shorter page)."""
        # Use custom page size that's 80% of letter height
        custom_height = self.LETTER_HEIGHT * 0.8
        c = canvas.Canvas(output_path, pagesize=(self.LETTER_WIDTH, custom_height))
        c.setTitle("ACORD 25 Partial Cutoff")

        # Adjust header position for shorter page
        original_header_y = self.HEADER_Y
        self.HEADER_Y = custom_height - 0.75 * inch

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(c)

        self.HEADER_Y = original_header_y
        c.showPage()
        c.save()

    def generate_faded_text(self, output_path):
        """Generate standard cert but text is rendered in very light gray."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("ACORD 25 Faded Text")

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(c, text_color=self.LIGHT_TEXT_COLOR)

        c.showPage()
        c.save()

    def generate_nonstandard_dates(self, output_path, date_format):
        """Generate standard cert with dates in specified format."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle(f"ACORD 25 Nonstandard Dates ({date_format})")

        # Convert certificate date based on format
        date_obj = datetime.strptime("01/15/2025", "%m/%d/%Y")

        date_mapping = {
            "MM/DD/YYYY": date_obj.strftime("%m/%d/%Y"),
            "MM-DD-YYYY": date_obj.strftime("%m-%d-%Y"),
            "Month DD, YYYY": date_obj.strftime("%B %d, %Y"),
            "DD/MM/YYYY": date_obj.strftime("%d/%m/%Y"),
            "MM/DD/YY": date_obj.strftime("%m/%d/%y"),
            "N/A": "N/A",
        }

        formatted_date = date_mapping.get(date_format, "01/15/2025")

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(c, certificate_date=formatted_date)

        c.showPage()
        c.save()

    def generate_borderline_coverage(self, output_path, gl_limit):
        """Generate standard cert with GL limit set to specified amount."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle(f"ACORD 25 Borderline Coverage ({gl_limit})")

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(c, gl_limit=gl_limit)

        c.showPage()
        c.save()

    def generate_old_acord_version(self, output_path):
        """Generate ACORD 25 (2010/06) format with slightly different layout."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("ACORD 25 (2010/06)")

        self._draw_standard_header(c, "ACORD 25 (2010/06)")
        # Same content but with older version indicator
        self._draw_standard_form_content(c)

        c.showPage()
        c.save()

    def generate_proprietary_form(self, output_path):
        """Generate a non-ACORD certificate (custom carrier form)."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("Proprietary Insurance Certificate")

        y = self.HEADER_Y
        c.setFont("Helvetica-Bold", 14)
        c.drawString(self.MARGIN, y, "CERTIFICATE OF INSURANCE")
        c.setFont("Helvetica", 9)
        c.drawString(self.MARGIN, y - 0.2 * inch, "XYZ Insurance Carrier - Proprietary Form")
        y -= 0.5 * inch

        c.setFont("Helvetica-Bold", 10)
        c.drawString(self.MARGIN, y, "INSURED:")
        c.setFont("Helvetica", 10)
        c.drawString(self.MARGIN + 1.2 * inch, y, "Test Construction LLC")
        y -= 0.3 * inch

        c.setFont("Helvetica-Bold", 10)
        c.drawString(self.MARGIN, y, "REQUESTING PARTY:")
        c.setFont("Helvetica", 10)
        c.drawString(self.MARGIN + 1.2 * inch, y, "General Contractor Inc")
        y -= 0.3 * inch

        c.setFont("Helvetica-Bold", 10)
        c.drawString(self.MARGIN, y, "ISSUED:")
        c.setFont("Helvetica", 10)
        c.drawString(self.MARGIN + 1.2 * inch, y, "01/15/2025")
        y -= 0.5 * inch

        # Proprietary layout
        c.setFont("Helvetica-Bold", 10)
        c.drawString(self.MARGIN, y, "COVERAGE DETAILS (Carrier-Specific Format)")
        y -= 0.25 * inch

        c.setLineWidth(0.5)
        c.line(self.MARGIN, y, self.LETTER_WIDTH - self.MARGIN, y)
        y -= 0.2 * inch

        c.setFont("Helvetica-Bold", 9)
        c.drawString(self.MARGIN, y, "Line of Business")
        c.drawString(self.MARGIN + 2.0 * inch, y, "Policy ID")
        c.drawString(self.MARGIN + 3.5 * inch, y, "Limit")
        y -= 0.25 * inch

        c.setFont("Helvetica", 9)
        coverage_data = [
            ("General Liability Coverage", "XYZ-GL-001", "$2,000,000"),
            ("Workers Compensation", "XYZ-WC-002", "$1,000,000"),
            ("Commercial General Liability", "XYZ-CGL-003", "$1,000,000"),
        ]

        for coverage, policy_id, limit in coverage_data:
            c.drawString(self.MARGIN, y, coverage)
            c.drawString(self.MARGIN + 2.0 * inch, y, policy_id)
            c.drawString(self.MARGIN + 3.5 * inch, y, limit)
            y -= 0.25 * inch

        c.showPage()
        c.save()

    def generate_cancellation_notice(self, output_path):
        """Generate a cancellation notice PDF."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("Notice of Cancellation")

        y = self.HEADER_Y
        c.setFont("Helvetica-Bold", 16)
        c.drawString(self.MARGIN, y, "NOTICE OF CANCELLATION")
        y -= 0.4 * inch

        c.setFont("Helvetica-Bold", 12)
        c.drawString(self.MARGIN, y, "CERTIFICATE CANCELLATION NOTICE")
        y -= 0.3 * inch

        c.setFont("Helvetica", 10)
        c.drawString(self.MARGIN, y, "This is to notify that the certificate of insurance issued to:")
        y -= 0.25 * inch

        c.setFont("Helvetica-Bold", 11)
        c.drawString(self.MARGIN + 0.25 * inch, y, "Test Construction LLC")
        y -= 0.3 * inch

        c.setFont("Helvetica", 10)
        c.drawString(self.MARGIN, y, "For coverage provided under Policy Number:")
        y -= 0.25 * inch

        c.setFont("Helvetica-Bold", 11)
        c.drawString(self.MARGIN + 0.25 * inch, y, "GL-12345")
        y -= 0.3 * inch

        c.setFont("Helvetica", 10)
        c.drawString(self.MARGIN, y, "will be CANCELLED effective:")
        y -= 0.25 * inch

        c.setFont("Helvetica-Bold", 11)
        c.drawString(self.MARGIN + 0.25 * inch, y, "02/15/2025")
        y -= 0.4 * inch

        c.setFont("Helvetica", 9)
        c.drawString(
            self.MARGIN,
            y,
            "Please note that cancellation will be effective as of the above date.",
        )
        y -= 0.2 * inch
        c.drawString(
            self.MARGIN,
            y,
            "No coverage will be provided after this date.",
        )

        c.showPage()
        c.save()

    def generate_missing_fields(self, output_path, missing):
        """Generate standard cert with specified fields omitted."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("ACORD 25 Missing Fields")

        include_insurer = "insurer_name" not in missing
        include_cert_holder = "certificate_holder" not in missing
        include_named_insured = "named_insured" not in missing
        include_policy = "policy_section" not in missing

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(
            c,
            include_insurer_name=include_insurer,
            include_certificate_holder=include_cert_holder,
            include_named_insured=include_named_insured,
            include_policy_section=include_policy,
        )

        c.showPage()
        c.save()

    def generate_blank_coverage(self, output_path):
        """Generate standard cert with coverage amount field blank."""
        c = canvas.Canvas(output_path, pagesize=letter)
        c.setTitle("ACORD 25 Blank Coverage")

        self._draw_standard_header(c, "ACORD 25 (2016/03)")
        self._draw_standard_form_content(c, blank_coverage=True)

        c.showPage()
        c.save()


def generate_all_test_pdfs(output_dir):
    """Generate all test PDFs into the specified directory."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    generator = AcordTestPDFGenerator()
    test_pdfs = {}

    # Standard ACORD 25
    path = os.path.join(output_dir, "standard_acord25.pdf")
    generator.generate_standard_acord25(path)
    test_pdfs["standard_acord25"] = path

    # Low resolution
    path = os.path.join(output_dir, "low_resolution.pdf")
    generator.generate_low_resolution(path)
    test_pdfs["low_resolution"] = path

    # Rotated
    path = os.path.join(output_dir, "rotated_5deg.pdf")
    generator.generate_rotated(path, angle=5)
    test_pdfs["rotated_5deg"] = path

    # Partial cutoff
    path = os.path.join(output_dir, "partial_cutoff.pdf")
    generator.generate_partial_cutoff(path)
    test_pdfs["partial_cutoff"] = path

    # Faded text
    path = os.path.join(output_dir, "faded_text.pdf")
    generator.generate_faded_text(path)
    test_pdfs["faded_text"] = path

    # Nonstandard dates
    date_formats = ["MM/DD/YYYY", "MM-DD-YYYY", "Month DD, YYYY", "DD/MM/YYYY", "MM/DD/YY", "N/A"]
    for fmt in date_formats:
        safe_fmt = fmt.replace("/", "_").replace(", ", "_").replace(" ", "_")
        path = os.path.join(output_dir, f"date_format_{safe_fmt}.pdf")
        generator.generate_nonstandard_dates(path, fmt)
        test_pdfs[f"date_format_{safe_fmt}"] = path

    # Borderline coverage amounts
    coverage_amounts = ["$1,000,000", "$999,999", "$1M", "1,000,000"]
    for amount in coverage_amounts:
        safe_amount = amount.replace("$", "").replace(",", "").replace("M", "million")
        path = os.path.join(output_dir, f"borderline_coverage_{safe_amount}.pdf")
        generator.generate_borderline_coverage(path, amount)
        test_pdfs[f"borderline_coverage_{safe_amount}"] = path

    # Old ACORD version
    path = os.path.join(output_dir, "old_acord_2010.pdf")
    generator.generate_old_acord_version(path)
    test_pdfs["old_acord_2010"] = path

    # Proprietary form
    path = os.path.join(output_dir, "proprietary_form.pdf")
    generator.generate_proprietary_form(path)
    test_pdfs["proprietary_form"] = path

    # Cancellation notice
    path = os.path.join(output_dir, "cancellation_notice.pdf")
    generator.generate_cancellation_notice(path)
    test_pdfs["cancellation_notice"] = path

    # Missing fields variants
    missing_variants = [
        (["insurer_name"], "missing_insurer"),
        (["certificate_holder"], "missing_cert_holder"),
        (["named_insured"], "missing_named_insured"),
        (["policy_section"], "missing_policy_section"),
        (
            ["insurer_name", "certificate_holder"],
            "missing_insurer_and_cert_holder",
        ),
    ]
    for missing_fields, variant_name in missing_variants:
        path = os.path.join(output_dir, f"{variant_name}.pdf")
        generator.generate_missing_fields(path, missing_fields)
        test_pdfs[variant_name] = path

    # Blank coverage
    path = os.path.join(output_dir, "blank_coverage.pdf")
    generator.generate_blank_coverage(path)
    test_pdfs["blank_coverage"] = path

    return test_pdfs


if __name__ == "__main__":
    generate_all_test_pdfs("test_pdfs")
