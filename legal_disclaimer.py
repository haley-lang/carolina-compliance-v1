"""
Legal Disclaimer — Carolina Compliance Solutions

Single source of truth for all legal disclaimer and footer text used in
outbound emails and compliance summaries.

Import from this module everywhere. Do NOT inline disclaimer copy in
individual email builders — if the language needs to change, it must
change in one place only.

Constants exported:
    EMAIL_DISCLAIMER             -- plain-text disclaimer for email bodies
    EMAIL_DISCLAIMER_HTML        -- HTML version for HTML emails
    SUMMARY_DISCLAIMER           -- short version for Compliance Decision Summaries
    SENDER_PHYSICAL_ADDRESS      -- CAN-SPAM required postal address
    SENDER_PHYSICAL_ADDRESS_HTML -- HTML version of the postal address
    SUPPORT_EMAIL                -- canonical support email address
    EMAIL_FOOTER                 -- standard plain-text footer block
    EMAIL_FOOTER_HTML            -- standard HTML footer block

Helpers exported:
    vendor_suppression_notice(client_name)       -- vendor opt-out text
    vendor_suppression_notice_html(client_name)  -- HTML version

Last reviewed: 2026-04-26 (pre-launch must-fix sweep)
"""

# -----------------------------------------------------------------------------
# CANONICAL DISCLAIMER — appended to every outbound email body
# -----------------------------------------------------------------------------
# Leading "\n\n---\n" preserved so existing callers that just append this
# constant to the end of an email body continue to work without modification.

EMAIL_DISCLAIMER = (
    "\n\n---\n"
    "Carolina Compliance Solutions is a software platform that helps general "
    "contractors organize and track Certificate of Insurance documentation "
    "submitted by their subcontractors. We are not an insurance broker, agent, "
    "advisor, or consultant. We do not verify coverage, determine coverage "
    "adequacy, or make insurance recommendations. The observations in this "
    "email reflect what our system was able to extract from the certificate "
    "documentation submitted to us, not a determination about your actual "
    "insurance coverage. For coverage questions, please contact your broker "
    "or insurance carrier directly."
)

EMAIL_DISCLAIMER_HTML = (
    '<hr style="margin-top:24px">'
    '<p style="font-size:11px;color:#999;line-height:1.4">'
    "Carolina Compliance Solutions is a software platform that helps general "
    "contractors organize and track Certificate of Insurance documentation "
    "submitted by their subcontractors. We are not an insurance broker, agent, "
    "advisor, or consultant. We do not verify coverage, determine coverage "
    "adequacy, or make insurance recommendations. The observations in this "
    "email reflect what our system was able to extract from the certificate "
    "documentation submitted to us, not a determination about your actual "
    "insurance coverage. For coverage questions, please contact your broker "
    "or insurance carrier directly."
    "</p>"
)

# -----------------------------------------------------------------------------
# COMPLIANCE DECISION SUMMARY — short footer for Airtable summaries
# -----------------------------------------------------------------------------
# Appears as the last line of every Compliance Decision Summary string.
# Customer-visible in the Softr portal; matches Section 6 rewrite spec.

SUMMARY_DISCLAIMER = (
    "Heads up: this is what we read from the certificate, not a determination "
    "about actual insurance coverage. For coverage questions, contact the "
    "sub's broker directly."
)

# -----------------------------------------------------------------------------
# CAN-SPAM PHYSICAL ADDRESS
# -----------------------------------------------------------------------------
# Federal CAN-SPAM Act requires a valid physical postal address in every
# commercial email. Must appear in every external-facing email footer.

SENDER_PHYSICAL_ADDRESS = (
    "Carolina Compliance Solutions LLC\n"
    "4030 Wake Forest Road, Ste 349\n"
    "Raleigh, NC 27609"
)

SENDER_PHYSICAL_ADDRESS_HTML = (
    "Carolina Compliance Solutions LLC<br>"
    "4030 Wake Forest Road, Ste 349<br>"
    "Raleigh, NC 27609"
)

# -----------------------------------------------------------------------------
# SUPPORT EMAIL
# -----------------------------------------------------------------------------

SUPPORT_EMAIL = "hello@carolinacompliancesolutions.com"

# -----------------------------------------------------------------------------
# STANDARD FOOTER BLOCKS
# -----------------------------------------------------------------------------
# Convenience constants combining address + support contact + disclaimer.
# New email builders should use these. Existing builders that already append
# EMAIL_DISCLAIMER directly do not need to migrate today.

EMAIL_FOOTER = (
    f"\n\n{SENDER_PHYSICAL_ADDRESS}\n"
    f"{SUPPORT_EMAIL}"
    f"{EMAIL_DISCLAIMER}"
)

EMAIL_FOOTER_HTML = (
    f'<p style="font-size:12px;color:#666;line-height:1.5;margin-top:24px">'
    f"{SENDER_PHYSICAL_ADDRESS_HTML}<br>"
    f'<a href="mailto:{SUPPORT_EMAIL}" style="color:#1B3A5C">{SUPPORT_EMAIL}</a>'
    f"</p>"
    f"{EMAIL_DISCLAIMER_HTML}"
)

# -----------------------------------------------------------------------------
# VENDOR-FACING SUPPRESSION NOTICE
# -----------------------------------------------------------------------------
# Append to vendor-facing emails (V1, V2, V3, V4, V6) so subs have a clear
# opt-out path for operational emails about a specific GC client.

def vendor_suppression_notice(client_name: str) -> str:
    """Return the vendor-facing suppression notice referencing the GC client."""
    return (
        f"If you'd prefer not to receive operational emails about your "
        f"subcontractor's COI, you can ask the general contractor whose "
        f"account this relates to ({client_name}) to remove your contact "
        f"information."
    )


def vendor_suppression_notice_html(client_name: str) -> str:
    """HTML version of the vendor-facing suppression notice."""
    return (
        f'<p style="font-size:11px;color:#666;line-height:1.5">'
        f"If you'd prefer not to receive operational emails about your "
        f"subcontractor's COI, you can ask the general contractor whose "
        f"account this relates to ({client_name}) to remove your contact "
        f"information."
        f"</p>"
    )
