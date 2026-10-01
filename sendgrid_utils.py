"""Shared SendGrid utilities — sandbox mode and send wrapper.

Usage in every send site:
    from sendgrid_utils import apply_sandbox_if_enabled, sandbox_active

    apply_sandbox_if_enabled(mail)   # mutates mail in-place; no-op when sandbox=false
    response = sg.send(mail)

SENDGRID_SANDBOX_MODE=true in .env or Railway env vars enables sandbox mode.
SendGrid accepts the request, logs it in the Activity Feed, and delivers nothing.
Response status is 200 in sandbox (vs 202 for real sends) — treat both as success.

Default is sandbox ON if the env var is missing entirely, so a deployment that
forgets to set the var fails safe rather than sending live.

Owner-only bypass: when SENDGRID_SANDBOX_MODE=true, emails whose complete
recipient list (To + CC + BCC across all personalizations) consists solely of
HALEY_EMAIL are delivered anyway. This lets owner-only system notifications
(new-signup alert, backup summary, import summary) reach Haley during pilots
without letting any vendor- or customer-facing mail through. If HALEY_EMAIL is
not set, no bypass occurs.
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env so this module works regardless of caller import order.
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=False)

logger = logging.getLogger(__name__)


def sandbox_active() -> bool:
    """Return True if SENDGRID_SANDBOX_MODE is enabled.

    Checked dynamically so Railway env var changes take effect without
    redeployment and so this works regardless of when .env is loaded.
    Default is True (safe) when the var is absent.
    """
    raw = os.getenv("SENDGRID_SANDBOX_MODE", "true").strip().lower()
    return raw == "true"


def _collect_all_recipients(mail) -> set:
    """Return a normalised set of every To/CC/BCC address on the mail object."""
    result = set()
    for pers in (mail.personalizations or []):
        for addr_list in (
            getattr(pers, "tos", None) or [],
            getattr(pers, "ccs", None) or [],
            getattr(pers, "bccs", None) or [],
        ):
            for item in addr_list:
                if isinstance(item, dict):
                    addr = item.get("email", "")
                elif hasattr(item, "email"):
                    addr = item.email or ""
                else:
                    addr = str(item)
                if addr:
                    result.add(addr.strip().lower())
    return result


def _describe_mail(mail) -> str:
    """Extract to/subject from a Mail object for logging. Never raises."""
    try:
        ps = mail.personalizations or []
        tos = ps[0].tos if ps else []
        to_str = ", ".join(t.get("email", "?") for t in tos) if tos else "?"
    except Exception:
        to_str = "?"
    try:
        subj = str(mail.subject) if mail.subject is not None else "?"
    except Exception:
        subj = "?"
    return f"to={to_str!r} subject={subj!r}"


def apply_sandbox_if_enabled(mail) -> None:
    """Mutate a Mail object to enable SendGrid sandbox mode if the env flag is set.

    Exception — owner-only bypass: if SENDGRID_SANDBOX_MODE=true but every
    recipient (To + CC + BCC) is exactly HALEY_EMAIL, sandbox is skipped so
    owner-only system notifications reach Haley during pilots. If HALEY_EMAIL
    is not set, no bypass occurs and all mail is sandboxed.

    Call this immediately before every sg.send() call.
    """
    if not sandbox_active():
        return

    owner = os.getenv("HALEY_EMAIL", "").strip().lower()
    if owner:
        recipients = _collect_all_recipients(mail)
        if recipients and all(r == owner for r in recipients):
            logger.info("[SENDGRID] Owner-only — bypassing sandbox — %s", _describe_mail(mail))
            return

    from sendgrid.helpers.mail import MailSettings, SandBoxMode  # noqa: avoid top-level dep

    settings = MailSettings()
    sb = SandBoxMode()
    sb.enable = True
    settings.sandbox_mode = sb
    mail.mail_settings = settings

    logger.warning("[SENDGRID SANDBOX] Suppressed send — %s", _describe_mail(mail))
