"""Tests for email_monitor._iter_coi_parts.

Covers the three acceptance cases:
  1. Content-Disposition: attachment (with and without filename)
  2. Content-Disposition: inline with explicit filename
  3. image/* / application/pdf with Content-ID but no filename (doubly-forwarded COIs)

And negative cases:
  - plain-text body parts
  - inline image with no filename AND no Content-ID
  - image with unknown subtype (e.g. svg+xml)

Also tests the message/rfc822 (forwarded-email wrapper) path, confirming that
walk() recurses into the nested message and finds the inner image.
"""
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from email_monitor import _iter_coi_parts


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _attach_part(msg, ctype, payload=b"data", disposition=None,
                 filename=None, content_id=None):
    """Build a MIMEBase part and attach it to msg."""
    maintype, subtype = ctype.split("/", 1)
    part = MIMEBase(maintype, subtype)
    part.set_payload(payload)
    encoders.encode_base64(part)
    if disposition:
        if filename:
            part.add_header("Content-Disposition", disposition, filename=filename)
        else:
            part.add_header("Content-Disposition", disposition)
    if content_id:
        part.add_header("Content-ID", content_id)
    msg.attach(part)
    return part


# ---------------------------------------------------------------------------
# Case 1 — explicit attachment
# ---------------------------------------------------------------------------

def test_explicit_pdf_attachment_is_found():
    msg = MIMEMultipart()
    msg.attach(MIMEText("Please see attached.", "plain"))
    _attach_part(msg, "application/pdf", filename="coi.pdf", disposition="attachment")

    results = list(_iter_coi_parts(msg))
    filenames = [f for f, _ in results if f]
    assert "coi.pdf" in filenames


def test_explicit_attachment_no_filename_yields_none_filename():
    """An attachment without a filename yields (None, part) so found_attachment
    can be set without attempting to save the file."""
    msg = MIMEMultipart()
    _attach_part(msg, "application/pdf", disposition="attachment")  # no filename

    results = list(_iter_coi_parts(msg))
    assert len(results) == 1
    fname, _ = results[0]
    assert fname is None


# ---------------------------------------------------------------------------
# Case 2 — inline with explicit filename
# ---------------------------------------------------------------------------

def test_inline_image_with_filename_is_found():
    msg = MIMEMultipart()
    msg.attach(MIMEText("Here is the COI.", "plain"))
    _attach_part(msg, "image/jpeg", filename="rocha_coi.jpg", disposition="inline")

    results = list(_iter_coi_parts(msg))
    filenames = [f for f, _ in results if f]
    assert "rocha_coi.jpg" in filenames


def test_inline_png_with_filename_is_found():
    msg = MIMEMultipart()
    _attach_part(msg, "image/png", filename="cert.png", disposition="inline")

    filenames = [f for f, _ in _iter_coi_parts(msg) if f]
    assert "cert.png" in filenames


# ---------------------------------------------------------------------------
# Case 3 — Content-ID, no filename (doubly-forwarded COI)
# ---------------------------------------------------------------------------

def test_cid_jpeg_no_filename_synthesises_jpg_extension():
    msg = MIMEMultipart()
    msg.attach(MIMEText("Please find the COI below.", "plain"))
    _attach_part(msg, "image/jpeg", content_id="<ii_xyz@mail.gmail.com>",
                 disposition="inline")

    results = list(_iter_coi_parts(msg))
    filenames = [f for f, _ in results if f]
    assert len(filenames) == 1
    assert filenames[0].endswith(".jpg")


def test_cid_png_no_filename_synthesises_png_extension():
    msg = MIMEMultipart()
    _attach_part(msg, "image/png", content_id="<img001@domain>", disposition="inline")

    filenames = [f for f, _ in _iter_coi_parts(msg) if f]
    assert len(filenames) == 1
    assert filenames[0].endswith(".png")


def test_cid_pdf_no_filename_synthesises_pdf_extension():
    msg = MIMEMultipart()
    part = MIMEBase("application", "pdf")
    part.set_payload(b"fake pdf bytes")
    part.add_header("Content-ID", "<doc@domain.com>")
    part.add_header("Content-Disposition", "inline")
    msg.attach(part)

    filenames = [f for f, _ in _iter_coi_parts(msg) if f]
    assert len(filenames) == 1
    assert filenames[0].endswith(".pdf")


def test_cid_stem_used_in_synthesised_filename():
    """The stem of the synthesised filename comes from the Content-ID local part."""
    msg = MIMEMultipart()
    _attach_part(msg, "image/jpeg", content_id="<mycoi123@gmail.com>", disposition="inline")

    filenames = [f for f, _ in _iter_coi_parts(msg) if f]
    assert filenames[0].startswith("mycoi123")


# ---------------------------------------------------------------------------
# Negative cases — parts that must NOT be found
# ---------------------------------------------------------------------------

def test_plain_text_body_not_found():
    msg = MIMEMultipart()
    msg.attach(MIMEText("Just some text.", "plain"))

    assert list(_iter_coi_parts(msg)) == []


def test_html_body_not_found():
    msg = MIMEMultipart()
    msg.attach(MIMEText("<p>Hello</p>", "html"))

    assert list(_iter_coi_parts(msg)) == []


def test_inline_image_no_filename_no_cid_is_skipped():
    """Inline part with no filename and no Content-ID must be ignored — it could
    be an embedded logo or signature image referenced by HTML src attribute."""
    msg = MIMEMultipart()
    part = MIMEBase("image", "jpeg")
    part.set_payload(b"logo bytes")
    part.add_header("Content-Disposition", "inline")  # no filename, no Content-ID
    msg.attach(part)

    assert list(_iter_coi_parts(msg)) == []


def test_cid_unknown_subtype_is_skipped():
    """image/svg+xml is not a COI format — unknown subtypes must not produce a part."""
    msg = MIMEMultipart()
    part = MIMEBase("image", "svg+xml")
    part.set_payload(b"<svg/>")
    part.add_header("Content-ID", "<logo@mail>")
    part.add_header("Content-Disposition", "inline")
    msg.attach(part)

    assert list(_iter_coi_parts(msg)) == []


def test_cid_text_type_is_skipped():
    """Non-image/non-application MIME types with Content-ID are not COI parts."""
    msg = MIMEMultipart()
    part = MIMEBase("text", "calendar")
    part.set_payload(b"BEGIN:VCALENDAR")
    part.add_header("Content-ID", "<cal@domain>")
    msg.attach(part)

    assert list(_iter_coi_parts(msg)) == []


# ---------------------------------------------------------------------------
# message/rfc822 — forwarded email with nested attachment
# ---------------------------------------------------------------------------

def _build_rfc822_wrapper(inner_msg):
    """Wrap inner_msg as a message/rfc822 attachment (how Gmail forwards)."""
    rfc_part = MIMEBase("message", "rfc822")
    rfc_part.set_payload([inner_msg])
    return rfc_part


def test_forwarded_rfc822_cid_image_no_filename_is_found():
    """Doubly-forwarded COI: outer email wraps original as message/rfc822;
    inner email has the COI image with Content-ID but no filename.

    This is exactly the Rocha/Lago Vista pattern that caused the missed intake."""
    inner = MIMEMultipart()
    inner.attach(MIMEText("Here is my certificate.", "plain"))
    _attach_part(inner, "image/jpeg", content_id="<coi_rocha@gmail.com>",
                 disposition="inline")

    outer = MIMEMultipart()
    outer.attach(MIMEText("Please see the forwarded COI below.", "plain"))
    outer.attach(_build_rfc822_wrapper(inner))

    filenames = [f for f, _ in _iter_coi_parts(outer) if f]
    assert len(filenames) == 1
    assert filenames[0].endswith(".jpg")


def test_forwarded_rfc822_explicit_attachment_inside_is_found():
    """Inner forwarded email has an explicit attachment — existing behavior preserved."""
    inner = MIMEMultipart()
    inner.attach(MIMEText("Please see attached.", "plain"))
    _attach_part(inner, "application/pdf", filename="rocha_insurance.pdf",
                 disposition="attachment")

    outer = MIMEMultipart()
    outer.attach(MIMEText("FWD:", "plain"))
    outer.attach(_build_rfc822_wrapper(inner))

    filenames = [f for f, _ in _iter_coi_parts(outer) if f]
    assert "rocha_insurance.pdf" in filenames


def test_forwarded_rfc822_inline_with_filename_inside_is_found():
    """Inner email has inline image with filename — existing behavior preserved."""
    inner = MIMEMultipart()
    _attach_part(inner, "image/jpeg", filename="cert.jpg", disposition="inline")

    outer = MIMEMultipart()
    outer.attach(_build_rfc822_wrapper(inner))

    filenames = [f for f, _ in _iter_coi_parts(outer) if f]
    assert "cert.jpg" in filenames


def test_multiple_attachments_all_found():
    """When an email has two attachments both should be yielded."""
    msg = MIMEMultipart()
    _attach_part(msg, "application/pdf", filename="coi1.pdf", disposition="attachment")
    _attach_part(msg, "image/jpeg", filename="coi2.jpg", disposition="inline")

    filenames = [f for f, _ in _iter_coi_parts(msg) if f]
    assert "coi1.pdf" in filenames
    assert "coi2.jpg" in filenames
    assert len(filenames) == 2
