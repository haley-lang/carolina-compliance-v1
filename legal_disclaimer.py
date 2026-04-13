"""
Legal Disclaimer — Carolina Compliance Solutions

Reusable disclaimer text appended to all outbound emails and compliance summaries.
This module is the single source of truth for legal language.
"""

# Plain text version — appended to every outbound email body
EMAIL_DISCLAIMER = (
    "\n\n---\n"
    "This assessment is based solely on certificate of insurance documentation "
    "submitted to Carolina Compliance Solutions. It does not constitute verification "
    "of actual insurance coverage, policy terms, or carrier obligations. "
    "Carolina Compliance Solutions is not an insurance advisor and does not determine "
    "coverage adequacy. Please consult your insurance advisor for coverage determinations."
)

# HTML version — for emails sent as HTML
EMAIL_DISCLAIMER_HTML = (
    '<hr style="margin-top:24px"><p style="font-size:11px;color:#999;line-height:1.4">'
    "This assessment is based solely on certificate of insurance documentation "
    "submitted to Carolina Compliance Solutions. It does not constitute verification "
    "of actual insurance coverage, policy terms, or carrier obligations. "
    "Carolina Compliance Solutions is not an insurance advisor and does not determine "
    "coverage adequacy. Please consult your insurance advisor for coverage determinations."
    "</p>"
)

# Short version — for Airtable compliance summaries
SUMMARY_DISCLAIMER = (
    "Based on submitted certificate documentation only. "
    "Not a determination of actual coverage. Consult your insurance advisor."
)
