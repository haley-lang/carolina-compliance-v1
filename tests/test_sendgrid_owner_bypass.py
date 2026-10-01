"""Tests for the owner-only sandbox bypass in sendgrid_utils.apply_sandbox_if_enabled.

Rules under test:
  - sandbox=false → always delivers (no mutation), regardless of recipients
  - sandbox=true, all recipients == HALEY_EMAIL → delivers (bypass)
  - sandbox=true, any recipient != HALEY_EMAIL → sandboxed
  - sandbox=true, HALEY_EMAIL not set → sandboxed (no bypass)
"""
import os

import pytest
from sendgrid.helpers.mail import Mail, Cc

from sendgrid_utils import apply_sandbox_if_enabled

OWNER = "haley@carolinacompliancesolutions.com"
OTHER = "vendor@example.com"
CUSTOMER = "customer@gc-example.com"


def _sandboxed(mail) -> bool:
    """Return True if the mail object has SendGrid sandbox mode enabled."""
    try:
        return bool(mail.mail_settings.sandbox_mode.enable)
    except (AttributeError, TypeError):
        return False


def _make_mail(to: str) -> Mail:
    return Mail(
        from_email="from@carolinacompliancesolutions.com",
        to_emails=to,
        subject="Test",
        plain_text_content="body",
    )


# ── sandbox off ───────────────────────────────────────────────────────────────


def test_sandbox_off_owner_delivers(monkeypatch):
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "false")
    monkeypatch.setenv("HALEY_EMAIL", OWNER)
    mail = _make_mail(OWNER)
    apply_sandbox_if_enabled(mail)
    assert not _sandboxed(mail)


def test_sandbox_off_customer_delivers(monkeypatch):
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "false")
    monkeypatch.setenv("HALEY_EMAIL", OWNER)
    mail = _make_mail(CUSTOMER)
    apply_sandbox_if_enabled(mail)
    assert not _sandboxed(mail)


# ── owner-only bypass ─────────────────────────────────────────────────────────


def test_owner_only_to_bypasses_sandbox(monkeypatch):
    """To = OWNER only, no CC/BCC → delivered even when sandbox=true."""
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "true")
    monkeypatch.setenv("HALEY_EMAIL", OWNER)
    mail = _make_mail(OWNER)
    apply_sandbox_if_enabled(mail)
    assert not _sandboxed(mail)


# ── mixed recipients stay sandboxed ──────────────────────────────────────────


def test_owner_plus_other_to_stays_sandboxed(monkeypatch):
    """To includes owner + a vendor → must stay sandboxed."""
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "true")
    monkeypatch.setenv("HALEY_EMAIL", OWNER)
    mail = _make_mail(OWNER)
    # Add a second To recipient
    from sendgrid.helpers.mail import To
    mail.personalizations[0].add_to(To(OTHER))
    apply_sandbox_if_enabled(mail)
    assert _sandboxed(mail)


def test_owner_to_other_cc_stays_sandboxed(monkeypatch):
    """To = owner but CC has a non-owner address → must stay sandboxed."""
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "true")
    monkeypatch.setenv("HALEY_EMAIL", OWNER)
    mail = _make_mail(OWNER)
    mail.personalizations[0].add_cc(Cc(OTHER))
    apply_sandbox_if_enabled(mail)
    assert _sandboxed(mail)


# ── non-owner recipients stay sandboxed ──────────────────────────────────────


def test_customer_only_stays_sandboxed(monkeypatch):
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "true")
    monkeypatch.setenv("HALEY_EMAIL", OWNER)
    mail = _make_mail(CUSTOMER)
    apply_sandbox_if_enabled(mail)
    assert _sandboxed(mail)


def test_vendor_only_stays_sandboxed(monkeypatch):
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "true")
    monkeypatch.setenv("HALEY_EMAIL", OWNER)
    mail = _make_mail(OTHER)
    apply_sandbox_if_enabled(mail)
    assert _sandboxed(mail)


# ── missing HALEY_EMAIL → no bypass ──────────────────────────────────────────


def test_missing_haley_email_stays_sandboxed(monkeypatch):
    """When HALEY_EMAIL is not set, even an owner-addressed mail is sandboxed."""
    monkeypatch.setenv("SENDGRID_SANDBOX_MODE", "true")
    monkeypatch.delenv("HALEY_EMAIL", raising=False)
    # Use the hardcoded default address — bypass must NOT fire without the env var
    mail = _make_mail(OWNER)
    apply_sandbox_if_enabled(mail)
    assert _sandboxed(mail)
