"""
Module 16 — Stripe Webhook Service
Handles checkout.session.completed events from Stripe to onboard new
customers (Airtable client record + single consolidated welcome email).

Inbound COI mail is handled separately by email_monitor.py (IMAP polling
of coi-intake@carolinacompliancesolutions.com). The previous SendGrid
Inbound Parse route was removed in favor of that path.
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

import stripe
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from pyairtable import Api as AirtableApi
from email_template import build_email_html

# Single source of truth for business hours — see module_21_business_hours.
from module_21_business_hours import (
    ET as EASTERN,
    BUSINESS_HOURS_START,
    BUSINESS_HOURS_END,
    BUSINESS_DAYS,
    is_business_hours as _is_business_hours_impl,
)

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
import config as _cfg
OWNER_EMAIL = _cfg.OWNER_EMAIL
SENDER_EMAIL = _cfg.FROM_EMAIL


def is_business_hours():
    # Delegates to module_21_business_hours (shared source of truth).
    return _is_business_hours_impl()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "business_hours": is_business_hours(),
        "time_et": datetime.now(EASTERN).strftime("%Y-%m-%d %H:%M:%S")
    }), 200


@app.route("/webhook/stripe-payment", methods=["POST"])
def stripe_payment():
    payload = request.get_data()
    sig_header = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        logger.warning("Invalid Stripe webhook signature")
        return jsonify({"error": "Invalid signature"}), 400

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        customer_email = session.customer_details.email if session.customer_details else "unknown"
        customer_name = session.customer_details.name if session.customer_details else "there"
        customer_phone = session.customer_details.phone if session.customer_details else "not provided"
        business_name = "not provided"
        if hasattr(session, 'custom_fields') and session.custom_fields:
            for field in session.custom_fields:
                if hasattr(field, 'text') and field.text:
                    business_name = field.text.value
        price_to_plan = {
            # Update these Price IDs from your Stripe dashboard.
            # V1 ships 3 tiers only — Starter / Growth / Scale. Pro is dead.
            "price_starter": "Starter",
            "price_growth": "Growth",
            "price_scale": "Scale",
        }
        # Stripe webhooks do not include line_items in the session payload by default.
        # Fetch them via API after receiving the event.
        # Use subscript access not .get() — Stripe Session objects raise
        # AttributeError when .get() is called on missing keys.
        try:
            session_id = session["id"]
        except (KeyError, TypeError):
            session_id = None

        price_id = ""
        if session_id:
            try:
                line_items = stripe.checkout.Session.list_line_items(session_id, limit=1)
                if line_items.data:
                    price_id = line_items.data[0].price.id
            except Exception as e:
                logger.exception(f"Failed to fetch line_items for session {session_id}: {e}")

        if not price_id:
            logger.warning(f"No price_id resolved for session {session_id}; falling back to amount-based plan mapping")

        plan = price_to_plan.get(price_id)
        if not plan:
            # 3-tier model only (Starter/Growth/Scale). If the amount doesn't
            # match, leave plan as None — caller logs and writes nothing to
            # Subscription Tier rather than guessing a wrong cap.
            plan = {14900: "Starter", 39900: "Growth", 79900: "Scale"}.get(session.amount_total)
            if plan:
                logger.warning("Plan detection fell back to amount-based matching. Amount: %s, Resolved: %s", session.amount_total, plan)
            else:
                logger.error("Could not resolve subscription tier for session %s (price_id=%s, amount=%s) — leaving Subscription Tier blank", session_id, price_id, session.amount_total)

        amount = session.amount_total / 100 if session.amount_total else 0

        logger.info("New customer: %s (%s) — %s — $%.2f/mo", customer_name, customer_email, plan, amount)
        _send_owner_notification(customer_name, customer_email, customer_phone, business_name, plan, amount)
        _create_airtable_client(customer_name, customer_email, business_name, amount, plan)
        _send_welcome_email(customer_name, customer_email, plan)

    return jsonify({"status": "ok"}), 200


def _create_airtable_client(customer_name, customer_email, business_name, amount, plan):
    """Create a new client record in Airtable after Stripe checkout completes."""
    try:
        if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
            logger.error("Airtable API key or Base ID not configured — skipping Airtable creation")
            return

        api = AirtableApi(AIRTABLE_API_KEY)
        clients_table = api.table(AIRTABLE_BASE_ID, "tbltnBIWke20IEI3K")

        # Check if client already exists with this email
        existing = clients_table.all(formula=f"{{fldmh1sYahgN5x6KQ}} = '{customer_email}'")
        if existing:
            logger.info("Client already exists for %s — skipping Airtable creation", customer_email)
            return

        # Determine client name: use business_name if provided, else customer_name
        client_name = business_name if business_name and business_name != "not provided" else customer_name

        today_str = datetime.now(EASTERN).strftime("%Y-%m-%d")

        record_fields = {
            "fldEZdqmIeahXDZHL": client_name,                        # Client Name
            "fldmh1sYahgN5x6KQ": customer_email,                     # Primary Contact Email
            "fldIWXSLRJYAVRs3P": customer_name,                      # Primary Contact Name
            "fldj4bxH9JwOSnGK8": customer_email,                     # Portal Email
            "fldlB9waYE5uPj1hI": amount,                             # Monthly Service Fee
            "flduyJEcYbWgFqpgg": today_str,                          # Service Start Date
            "fldCL7YwtBAG7slEd": "Active",                           # Client Status
            "flde7mzXePBqKBjGx": "Pending — Awaiting Reply",        # Requirements Status
        }
        # Subscription Tier (single-select: Starter / Growth / Scale).
        # Only write if we resolved a known tier; bulk_vendor_import enforces
        # the cap on read, and a missing value will hard-fail there with a
        # clear message rather than silently allowing an unbounded import.
        if plan in {"Starter", "Growth", "Scale"}:
            record_fields["fldXnhsv3ntup5Gpy"] = plan

        clients_table.create(record_fields)
        logger.info("Created Airtable client record for %s (tier=%s)", customer_email, plan)

    except Exception as e:
        logger.error("Failed to create Airtable client record for %s: %s", customer_email, e)


def _send_owner_notification(name, email, phone, business, plan, amount):
    import sendgrid
    from sendgrid.helpers.mail import Mail, Email as SGEmail
    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=OWNER_EMAIL,
        subject=f"🎉 New Customer: {name} — {plan}",
        html_content=f"""
        <h2>New paying customer!</h2>
        <p><strong>Name:</strong> {name}</p>
        <p><strong>Business:</strong> {business}</p>
        <p><strong>Email:</strong> {email}</p>
        <p><strong>Phone:</strong> {phone}</p>
        <p><strong>Plan:</strong> {plan}</p>
        <p><strong>Amount:</strong> ${amount:.2f}/mo</p>
        <hr>
        <p>Next step: Add them manually in Softr at app.carolinacompliancesolutions.com</p>
        """
    )
    message.reply_to = SGEmail(_cfg.reply_to_for("internal"))
    sg.send(message)
    logger.info("Owner notification sent for %s", email)


def _has_welcome_been_sent_recently(email: str) -> bool:
    try:
        clients_table = AirtableApi(AIRTABLE_API_KEY).table(AIRTABLE_BASE_ID, "tbltnBIWke20IEI3K")
        formula = f"AND({{Portal Email}}='{email}', {{Welcome Sent}}=TRUE())"
        records = clients_table.all(formula=formula)
        return len(records) > 0
    except Exception as e:
        logger.warning("Welcome dedup check failed: %s", e)
        return False


def _record_welcome_email_sent(email: str) -> None:
    try:
        clients_table = AirtableApi(AIRTABLE_API_KEY).table(AIRTABLE_BASE_ID, "tbltnBIWke20IEI3K")
        formula = f"{{Portal Email}}='{email}'"
        records = clients_table.all(formula=formula)
        if records:
            clients_table.update(records[0]["id"], {"Welcome Sent": True})
    except Exception as e:
        logger.warning("Failed to record welcome sent: %s", e)


def _send_welcome_email(name, email, plan):
    """Send the single consolidated welcome email after Stripe checkout.

    Replaces the previous welcome + onboarding two-email flow. Takes the
    Stripe-resolved plan ("Starter" / "Growth" / "Scale") and personalizes
    the signup line; falls back to a no-tier line if plan is unknown.
    """
    # Idempotency check: skip if welcome email already sent (Airtable lookup)
    if _has_welcome_been_sent_recently(email):
        logger.info("Welcome email already sent to %s — skipping.", email)
        return

    import sendgrid
    from sendgrid.helpers.mail import Mail, Email as SGEmail
    from legal_disclaimer import EMAIL_DISCLAIMER_HTML
    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)

    # First name from the Stripe customer name (split on first space).
    # Stripe's "not provided" + empty string both fall back to no-name greeting.
    first_name = ""
    if name and name.strip() and name.strip().lower() not in {"there", "not provided"}:
        first_name = name.strip().split(" ", 1)[0]

    greeting = f"<p>Hi {first_name},</p>" if first_name else "<p>Hi,</p>"

    if plan in {"Starter", "Growth", "Scale"}:
        signup_line = (
            f"<p>Welcome to Carolina Compliance Solutions. You just signed up for "
            f"<strong>{plan}</strong> — thanks for trusting us with this.</p>"
        )
    else:
        signup_line = (
            "<p>Welcome to Carolina Compliance Solutions — thanks for trusting "
            "us with this.</p>"
        )

    divider = '<hr style="border:none;border-top:1px solid #1B3A5C;margin:24px 0;">'

    welcome_subject = "You're in — here's what happens next"
    welcome_body_html = (
        greeting
        + signup_line
        + "<p>Here's the short version of what we do: <strong>we chase your "
          "subs for their certificates so you don't have to.</strong> Once we "
          "know who your subs are and what coverage you require, we handle "
          "the COI requests, the expiration tracking, the renewal follow-ups. "
          "You see the results in one dashboard.</p>"
        + "<p>To get started, I need two things from you. The faster you send "
          "these, the faster the system kicks in for you.</p>"
        + "<p><strong>1. Your subcontractor list</strong></p>"
        + "<p>Reply to this email with however you have it — a spreadsheet or "
          "a list typed in the email body. For each sub, the most useful "
          "info is:</p>"
        + "<ul>"
          "<li>Company name</li>"
          "<li>Email address (the one that actually handles their COIs — "
          "sometimes that's the office, sometimes the owner, sometimes their "
          "insurance agent)</li>"
          "<li>Phone (optional, but helpful if email bounces)</li>"
          "</ul>"
        + "<p>If you already have COIs on file for some of your subs, forward "
          "those PDFs to <strong>coi@carolinacompliancesolutions.com</strong> "
          "and we'll process them automatically. No need to organize them — "
          "just forward.</p>"
        + "<p><strong>2. Your insurance requirements</strong></p>"
        + "<p>Reply to this email and tell me what you require from your "
          "subs. The basics:</p>"
        + "<ul>"
          "<li>General Liability — required? If yes, what minimum limit?</li>"
          "<li>Workers Compensation — required?</li>"
          "<li>Auto Liability — required? If yes, what minimum limit?</li>"
          "<li>Additional Insured endorsement — required?</li>"
          "<li>Waiver of Subrogation — required?</li>"
          "</ul>"
        + "<p>If you have a written requirements document or contract "
          "template, attach it instead — I'll pull what I need.</p>"
        + divider
        + "<p><strong>Once I have those, here's what happens:</strong></p>"
        + "<ul>"
          "<li>Within 1 business day, COI request emails start going out to "
          "your subs automatically</li>"
          "<li>As they reply with certificates, our system extracts the "
          "policy details and evaluates compliance against your requirements</li>"
          "<li>If a sub's coverage doesn't match what you require, we send "
          "them a follow-up. If they don't respond, we keep following up.</li>"
          "<li>You can log in to "
          "<strong><a href=\"https://app.carolinacompliancesolutions.com\">"
          "app.carolinacompliancesolutions.com</a></strong> to see the status "
          "of every sub at any time</li>"
          "<li>Whenever you need to know which subs have current "
          "documentation on file and which don't, the answer is one click "
          "away</li>"
          "</ul>"
        + divider
        + "<p><strong>Something to know:</strong></p>"
        + "<p>You're going to hear from me directly. Not a ticket queue, not "
          "a chatbot. I'm Haley — I built this and I run it. When you reply "
          "to this email, I read it. When you have a question, I answer it.</p>"
        + "<p>Reply with your sub list and requirements whenever you're "
          "ready. If you have questions first, those are welcome too.</p>"
        + "<p>Talk soon,</p>"
        + "<p>Haley Bridges<br>"
          "Founder, Carolina Compliance Solutions<br>"
          f"{_cfg.OWNER_EMAIL}<br>"
          "carolinacompliancesolutions.com</p>"
        + EMAIL_DISCLAIMER_HTML
    )

    message = Mail(
        from_email=SGEmail(email=SENDER_EMAIL, name="Carolina Compliance Solutions"),
        to_emails=email,
        subject=welcome_subject,
        html_content=build_email_html(welcome_subject, welcome_body_html, audience="client"),
    )
    message.reply_to = SGEmail(_cfg.reply_to_for("client"))
    sg.send(message)
    _record_welcome_email_sent(email)
    logger.info("Welcome email sent to %s (tier=%s)", email, plan)


if __name__ == "__main__":
    logger.info("=== Module 16: Stripe Webhook Service starting ===")
    logger.info("Business hours: Mon-Fri %dam-%dpm ET", BUSINESS_HOURS_START, BUSINESS_HOURS_END)
    port = int(os.environ.get("PORT", 5051))
    app.run(host="0.0.0.0", port=port, debug=False)
