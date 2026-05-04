"""
Reusable branded HTML email template for Carolina Compliance Solutions.

Usage:
    from email_template import build_email_html

    html = build_email_html(
        subject="Action Needed: COI for Acme Builders",
        body_html="<p>Your certificate of insurance is missing.</p>",
        gc_company_name="Acme Builders",   # optional
    )
"""


import config as _cfg


def build_email_html(subject: str, body_html: str, gc_company_name: str = None,
                     audience: str = "vendor") -> str:
    """Return a complete HTML email string with CCS branding.

    Parameters
    ----------
    subject : str
        Used only in the hidden preview/preheader text.
    body_html : str
        Inner HTML content to display in the white body area.
    gc_company_name : str, optional
        If provided, a light-gray context bar is shown below the header:
        "[gc_company_name] uses Carolina Compliance Solutions to manage
        subcontractor insurance compliance."
    audience : str
        Drives which address shows in the footer/contact line. "vendor"
        (default) → INBOUND_EMAIL — composing new mail lands at the COI
        ingest mailbox (correct for vendors). "client" / "internal" →
        OWNER_EMAIL so a client/owner reaches Haley directly instead of
        the automated inbox.
    """
    footer_email = _cfg.reply_to_for(audience)

    gc_bar = ""
    if gc_company_name:
        gc_bar = (
            '<tr>'
            '<td style="background-color:#F2F4F7;padding:10px 30px;'
            'font-family:Arial,sans-serif;font-size:12px;color:#555555;'
            'text-align:center;">'
            f'{gc_company_name} uses Carolina Compliance Solutions to manage '
            'subcontractor insurance compliance.'
            '</td>'
            '</tr>'
        )

    # HTML-based header. SVG previously used here was being stripped/squashed
    # by Gmail and Outlook, rendering the brand text as
    # "CAROLINACOMPLIANCE SOLUTIONSLess chasing. More building." with no
    # spacing. Plain HTML + table-based styling renders consistently across
    # email clients.
    header_html = (
        '<td style="background-color:#1B3A5C;padding:28px 30px;text-align:center;">'
        '<div style="font-family:Georgia,serif;font-size:24px;font-weight:700;'
        'letter-spacing:3px;color:#FFFFFF;line-height:1.2;">'
        'CAROLINA COMPLIANCE SOLUTIONS'
        '</div>'
        '<div style="font-family:Georgia,serif;font-size:13px;font-style:italic;'
        'color:#7BAFD4;letter-spacing:1px;margin-top:10px;">'
        'Less chasing. More building.'
        '</div>'
        '</td>'
    )

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{subject}</title>
<!--[if mso]>
<style>table,td {{font-family:Arial,sans-serif;}}</style>
<![endif]-->
</head>
<body style="margin:0;padding:0;background-color:#EAEEF3;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;">
<!-- Hidden preheader text -->
<div style="display:none;max-height:0;overflow:hidden;">{subject}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
       style="background-color:#EAEEF3;">
<tr><td align="center" style="padding:20px 10px;">
<!-- Main container -->
<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"
       style="max-width:600px;width:100%;border-collapse:collapse;">
<!-- Header -->
<tr>
{header_html}
</tr>
{gc_bar}
<!-- Body -->
<tr>
<td style="background-color:#FFFFFF;padding:35px 30px;font-family:Arial,sans-serif;
font-size:15px;line-height:1.6;color:#333333;">
{body_html}
</td>
</tr>
<!-- Divider -->
<tr>
<td style="background-color:#FFFFFF;padding:0 30px;">
<hr style="border:none;border-top:1px solid #1B3A5C;margin:0;">
</td>
</tr>
<!-- Footer -->
<tr>
<td style="background-color:#FFFFFF;padding:20px 30px 25px;text-align:center;
font-family:Arial,sans-serif;font-size:12px;color:#999999;font-style:italic;">
carolinacompliancesolutions.com &nbsp;|&nbsp;
{footer_email} &nbsp;|&nbsp;
Gaston County, NC
</td>
</tr>
</table>
<!-- /Main container -->
</td></tr>
</table>
</body>
</html>"""
