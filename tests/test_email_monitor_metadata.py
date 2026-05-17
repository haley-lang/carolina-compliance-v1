"""Unit tests for email_monitor._parse_email_metadata.

Covers CC parsing (simple/missing/encoded/comma-in-name), Message-ID
preservation, body extraction (plain/HTML/empty), truncation, and a
belt-and-suspenders garbage-input sanity test.
"""
from email import message_from_string

from email_monitor import _parse_email_metadata


def _make_msg(headers: dict, body: str = "") -> "Message":
    """Construct an email.message.Message from a header dict + body string."""
    header_lines = "\n".join(f"{k}: {v}" for k, v in headers.items())
    raw = f"{header_lines}\n\n{body}"
    return message_from_string(raw)


# ── CC parsing ────────────────────────────────────────────────────────────────


def test_simple_cc_single_address():
    msg = _make_msg({"From": "s@x.com", "Cc": "agent@broker.com"})
    md = _parse_email_metadata(msg)
    assert md["cc"] == "agent@broker.com"


def test_missing_cc_returns_empty_string():
    msg = _make_msg({"From": "s@x.com"})
    md = _parse_email_metadata(msg)
    assert md["cc"] == ""


def test_cc_with_display_name():
    msg = _make_msg({
        "Cc": '"Jane Smith" <jane@example.com>, broker@example.com',
    })
    md = _parse_email_metadata(msg)
    # Both entries preserved; display name kept where present
    assert '"Jane Smith" <jane@example.com>' in md["cc"]
    assert "broker@example.com" in md["cc"]


def test_cc_with_encoded_display_name():
    # =?UTF-8?Q?Jos=C3=A9?= decodes to "José"
    msg = _make_msg({
        "Cc": "=?UTF-8?Q?Jos=C3=A9?= <jose@example.com>",
    })
    md = _parse_email_metadata(msg)
    assert "José" in md["cc"]
    assert "<jose@example.com>" in md["cc"]


def test_cc_with_comma_in_display_name():
    # Naive split(",") would break on this; getaddresses handles it.
    msg = _make_msg({
        "Cc": '"Smith, John" <john@example.com>, "Doe, Jane" <jane@example.com>',
    })
    md = _parse_email_metadata(msg)
    assert "john@example.com" in md["cc"]
    assert "jane@example.com" in md["cc"]


# ── Message-ID ────────────────────────────────────────────────────────────────


def test_message_id_preserved_with_brackets():
    msg = _make_msg({"Message-ID": "<abc123@mail.gmail.com>"})
    md = _parse_email_metadata(msg)
    assert md["message_id"] == "<abc123@mail.gmail.com>"


def test_missing_message_id_returns_empty_string():
    msg = _make_msg({"From": "s@x.com"})
    md = _parse_email_metadata(msg)
    assert md["message_id"] == ""


# ── Body extraction ──────────────────────────────────────────────────────────


def test_plain_text_body_passes_through():
    msg = _make_msg({}, body="Hello, please find the COI attached.")
    md = _parse_email_metadata(msg)
    assert "Hello, please find the COI attached." in md["body_snippet"]


def test_html_only_body_strips_tags():
    raw = (
        "MIME-Version: 1.0\n"
        "Content-Type: text/html; charset=utf-8\n"
        "\n"
        "<html><body><p>Please find the COI attached.</p></body></html>"
    )
    msg = message_from_string(raw)
    md = _parse_email_metadata(msg)
    # Tags stripped, text preserved
    assert "Please find the COI attached" in md["body_snippet"]
    assert "<p>" not in md["body_snippet"]
    assert "<html>" not in md["body_snippet"]


def test_empty_body_returns_empty_string():
    msg = _make_msg({"From": "s@x.com"}, body="")
    md = _parse_email_metadata(msg)
    assert md["body_snippet"] == ""


def test_body_snippet_truncates_at_500():
    """Snippet must not exceed 500 chars regardless of input length.

    .strip() may eat 1-2 chars off the slice if email parsing inserted
    whitespace; the cap (<= 500) is what matters, not exact equality.
    """
    long_body = "X" * 1000
    msg = _make_msg({"From": "s@x.com"}, body=long_body)
    md = _parse_email_metadata(msg)
    assert 490 <= len(md["body_snippet"]) <= 500


# ── Garbage-input sanity check ───────────────────────────────────────────────


def test_completely_malformed_message_returns_empty_dict():
    """Belt-and-suspenders for the IMAP-returns-garbage case.

    A Message object built from an empty string has no headers, no body.
    Should return cc/message_id/body_snippet all empty without raising.
    """
    msg = message_from_string("")
    md = _parse_email_metadata(msg)
    assert md == {"cc": "", "message_id": "", "body_snippet": ""}
