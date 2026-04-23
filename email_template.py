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


def build_email_html(subject: str, body_html: str, gc_company_name: str = None) -> str:
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
    """

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

    logo_svg = (
        '<svg width="260" height="65" viewBox="0 0 320 80" fill="none" '
        'xmlns="http://www.w3.org/2000/svg">'
        '<path d="M34 4C34 4 12 12 10 14C10 14 8 16 8 22C8 38 14 58 34 68'
        'C54 58 60 38 60 22C60 16 58 14 58 14C56 12 34 4 34 4Z" fill="#4A90D9"/>'
        '<path d="M34 8C34 8 15 15 13.5 16.5C13.5 16.5 12 18 12 23C12 37 17.5 55 '
        '34 64C50.5 55 56 37 56 23C56 18 54.5 16.5 54.5 16.5C53 15 34 8 34 8Z" '
        'fill="#5A9FE8"/>'
        '<path d="M34 13C34 13 18 19.5 17 20.5C17 20.5 16 22 16 26C16 38 20 53 '
        '34 61C48 53 52 38 52 26C52 22 51 20.5 51 20.5C50 19.5 34 13 34 13Z" '
        'fill="#4A90D9"/>'
        '<polyline points="22,38 29,47 47,28" stroke="white" stroke-width="3.5" '
        'stroke-linecap="round" stroke-linejoin="round" fill="none"/>'
        '<text x="78" y="32" font-family="Georgia, serif" font-size="28" '
        'font-weight="700" fill="white" letter-spacing="2">CAROLINA</text>'
        '<text x="79" y="50" font-family="Arial, sans-serif" font-size="13" '
        'font-weight="500" fill="#7BAFD4" letter-spacing="3">COMPLIANCE SOLUTIONS</text>'
        '<line x1="79" y1="58" x2="310" y2="58" stroke="#2A5580" stroke-width="0.75"/>'
        '<text x="79" y="72" font-family="Georgia, serif" font-size="11" '
        'font-style="italic" fill="#7BAFD4" letter-spacing="0.5">'
        'Less chasing. More building.</text>'
        '</svg>'
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
<td style="background-color:#1B3A5C;padding:12px 30px;height:80px;vertical-align:middle;">
{logo_svg}
</td>
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
{_cfg.INBOUND_EMAIL} &nbsp;|&nbsp;
Gaston County, NC
</td>
</tr>
</table>
<!-- /Main container -->
</td></tr>
</table>
</body>
</html>"""
