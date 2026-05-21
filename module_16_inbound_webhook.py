"""
Module 16 — Stripe Webhook Service
Handles Stripe events to track customer subscription lifecycle in Airtable:

    checkout.session.completed     -> create Client record, welcome email,
                                       write Stripe IDs + Subscription Status
                                       (Trial if coupon used, else Active) +
                                       Trial Ends (if trial) + Tier + Coupon Used
    customer.subscription.deleted  -> mark Subscription Status = Canceled,
                                       record Subscription Ended date, send
                                       cancellation confirmation email
    invoice.payment_failed         -> mark Subscription Status = Past Due
                                       (no email — Stripe dunning handles)
    invoice.paid                   -> if Past Due, flip back to Active
    customer.subscription.updated  -> handle cancel-scheduled (log only),
                                       tier change (write Tier+Subscription Tier),
                                       coupon-expired post-trial (flip Trial->Active)

Inbound COI mail is handled separately by email_monitor.py (IMAP polling
of coi-intake@carolinacompliancesolutions.com). The previous SendGrid
Inbound Parse route was removed in favor of that path.

MANUAL TEST CHECKLIST (Stripe CLI):
    stripe listen --forward-to localhost:5051/webhook/stripe-payment
    stripe trigger checkout.session.completed
    stripe trigger customer.subscription.deleted
    stripe trigger invoice.payment_failed
    stripe trigger invoice.paid
    stripe trigger customer.subscription.updated
After each, verify the Airtable Client record fields match the spec.

STRIPE DASHBOARD CONFIG (NOT done in code — verify manually):
    1. Customer-initiated cancellations: cancel at period end (not immediately)
    2. Webhook endpoint enabled events: customer.subscription.deleted,
       invoice.payment_failed, invoice.paid, customer.subscription.updated
    3. Smart Retries (dunning) enabled with auto-cancel on final failure
"""

import os
import logging
from datetime import datetime, timedelta, timezone
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

# ── Airtable: Clients table + field IDs (verified via MCP on 2026-05-20) ────
# Field IDs are the source of truth. Using IDs (not names) so field renames
# in Airtable don't break this module.
CLIENTS_TABLE_ID                  = "tbltnBIWke20IEI3K"
FLD_CLIENT_NAME                   = "fldEZdqmIeahXDZHL"  # Client Name (singleLineText)
FLD_CLIENT_PRIMARY_EMAIL          = "fldmh1sYahgN5x6KQ"  # Primary Contact Email (email)
FLD_CLIENT_STRIPE_CUSTOMER_ID     = "fldcc5fhvHAj48ImQ"  # Stripe Customer ID (singleLineText)
FLD_CLIENT_STRIPE_SUBSCRIPTION_ID = "fldvnOMoyk6WlUxwp"  # Stripe Subscription ID (singleLineText)
FLD_CLIENT_SUBSCRIPTION_STATUS    = "fld4ZGRZ71BWabXwC"  # Subscription Status (singleSelect: Active/Trial/Past Due/Canceled/Archived)
FLD_CLIENT_SUBSCRIPTION_STARTED   = "fldQLzK7UjIMD3dba"  # Subscription Started (date)
FLD_CLIENT_TRIAL_ENDS             = "fldbTTEx3e1myoMr2"  # Trial Ends (date)
FLD_CLIENT_SUBSCRIPTION_ENDED     = "fld9SrkYleqvkUaHO"  # Subscription Ended (date)
FLD_CLIENT_TIER                   = "fld57t54w0FMqN32C"  # Tier (singleSelect: Starter/Growth/Scale)
FLD_CLIENT_COUPON_USED            = "fldVVIOEwJBCeL9MX"  # Coupon Used (singleLineText)
FLD_CLIENT_SUBSCRIPTION_TIER      = "fldXnhsv3ntup5Gpy"  # Subscription Tier (singleSelect) — kept in sync with Tier for bulk_vendor_import compatibility

# ── Stripe price → tier mapping (3-tier model: Starter / Growth / Scale) ────
# Module-level so multiple handlers (checkout, subscription.updated) can
# reuse it. Update Price IDs from Stripe dashboard when prices change.
PRICE_ID_TO_TIER = {
    "price_starter": "Starter",
    "price_growth":  "Growth",
    "price_scale":   "Scale",
}
# Fallback when price_id is unresolved — match by Stripe amount_total (cents).
AMOUNT_CENTS_TO_TIER = {14900: "Starter", 39900: "Growth", 79900: "Scale"}


def _resolve_tier(price_id: str = "", amount_cents=None):
    """Map a Stripe price ID (or fallback amount) to one of Starter/Growth/Scale.
    Returns None if neither resolves — caller should log + skip the tier write.
    """
    if price_id and price_id in PRICE_ID_TO_TIER:
        return PRICE_ID_TO_TIER[price_id]
    if amount_cents in AMOUNT_CENTS_TO_TIER:
        return AMOUNT_CENTS_TO_TIER[amount_cents]
    return None


def _clients_table():
    """Construct a pyairtable Table for Clients. Used by all subscription
    lifecycle handlers. Does not cache — pyairtable.Api is lightweight."""
    api = AirtableApi(AIRTABLE_API_KEY)
    return api.table(AIRTABLE_BASE_ID, CLIENTS_TABLE_ID)


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

    event_type = event.get("type")

    # ── Dispatch — each handler catches its own exceptions and logs to ERROR.
    # Webhook returns 200 in all cases to prevent Stripe retries on partial
    # failures (retries can cause double-side-effects: duplicate emails, etc.).
    try:
        if event_type == "checkout.session.completed":
            _handle_checkout_completed(event)
        elif event_type == "customer.subscription.deleted":
            _handle_subscription_deleted(event)
        elif event_type == "invoice.payment_failed":
            _handle_invoice_payment_failed(event)
        elif event_type == "invoice.paid":
            _handle_invoice_paid(event)
        elif event_type == "customer.subscription.updated":
            _handle_subscription_updated(event)
        else:
            logger.info("Ignoring unhandled Stripe event type: %s", event_type)
    except Exception as exc:
        # Defensive outer catch — handler-level catches should normally
        # contain failures, but if anything escapes (e.g. KeyError on a
        # malformed payload), log and still 200 so Stripe doesn't retry.
        logger.exception("Unhandled exception in webhook dispatch for %s: %s", event_type, exc)

    return jsonify({"status": "ok"}), 200


def _handle_checkout_completed(event):
    """Process checkout.session.completed — create Client record, send welcome
    email, write Stripe IDs + subscription lifecycle fields."""
    session = event["data"]["object"]
    customer_email = session.customer_details.email if session.customer_details else "unknown"
    customer_name = session.customer_details.name if session.customer_details else "there"
    customer_phone = session.customer_details.phone if session.customer_details else "not provided"
    business_name = "not provided"
    if hasattr(session, 'custom_fields') and session.custom_fields:
        for field in session.custom_fields:
            if hasattr(field, 'text') and field.text:
                business_name = field.text.value

    # Stripe webhooks don't include line_items by default — fetch separately.
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

    plan = _resolve_tier(price_id=price_id, amount_cents=session.amount_total)
    if not plan:
        logger.error(
            "Could not resolve subscription tier for session %s (price_id=%s, amount=%s) — Subscription Tier left blank",
            session_id, price_id, session.amount_total,
        )

    amount = session.amount_total / 100 if session.amount_total else 0

    # Stripe Customer + Subscription IDs land on the session for subscription-mode checkouts.
    stripe_customer_id = session.get("customer") if hasattr(session, "get") else getattr(session, "customer", None)
    stripe_subscription_id = session.get("subscription") if hasattr(session, "get") else getattr(session, "subscription", None)

    # Coupon detection requires a separate retrieve with expand (webhook
    # payload doesn't include nested discount details). Returns "" if no
    # coupon was applied or if the lookup fails.
    coupon_code = _extract_coupon_code(session_id)
    is_trial = bool(coupon_code)

    logger.info(
        "New customer: %s (%s) — %s — $%.2f/mo — coupon=%r — stripe_customer=%s sub=%s",
        customer_name, customer_email, plan, amount, coupon_code,
        stripe_customer_id, stripe_subscription_id,
    )
    _send_owner_notification(customer_name, customer_email, customer_phone, business_name, plan, amount)
    _create_airtable_client(
        customer_name=customer_name,
        customer_email=customer_email,
        business_name=business_name,
        amount=amount,
        plan=plan,
        stripe_customer_id=stripe_customer_id,
        stripe_subscription_id=stripe_subscription_id,
        is_trial=is_trial,
        coupon_code=coupon_code,
    )
    _send_welcome_email(customer_name, customer_email, plan)


def _extract_coupon_code(session_id: str) -> str:
    """Retrieve the session with expand and pull the coupon ID (or
    promotion_code code) if any. Returns "" if no coupon, lookup fails,
    or the session has no discounts.
    """
    if not session_id:
        return ""
    try:
        full = stripe.checkout.Session.retrieve(
            session_id,
            expand=["total_details.breakdown.discounts"],
        )
        td = getattr(full, "total_details", None)
        breakdown = getattr(td, "breakdown", None) if td else None
        discounts = getattr(breakdown, "discounts", None) if breakdown else None
        if not discounts:
            return ""
        first = discounts[0]
        # Each discount entry has .discount (the actual Discount object) which has .coupon and optionally .promotion_code
        d = getattr(first, "discount", None)
        if not d:
            return ""
        # Prefer the human-readable promotion_code if available
        pc = getattr(d, "promotion_code", None)
        if pc:
            if isinstance(pc, str):
                return pc
            code = getattr(pc, "code", None)
            if code:
                return code
        coupon = getattr(d, "coupon", None)
        if coupon:
            return getattr(coupon, "id", "") or ""
        return ""
    except Exception as e:
        logger.warning("Could not extract coupon code for session %s: %s", session_id, e)
        return ""


def _create_airtable_client(customer_name, customer_email, business_name, amount, plan,
                             stripe_customer_id=None, stripe_subscription_id=None,
                             is_trial=False, coupon_code=""):
    """Create a new client record in Airtable after Stripe checkout completes.

    Writes Stripe Customer / Subscription IDs, Subscription Status (Trial if
    a coupon was applied, else Active), Subscription Started (today), Trial
    Ends (today + 30 days, only if trial), Tier + Subscription Tier (kept in
    sync for bulk_vendor_import cap enforcement), and Coupon Used.
    """
    try:
        if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
            logger.error("Airtable API key or Base ID not configured — skipping Airtable creation")
            return

        clients_table = _clients_table()

        # Check if client already exists with this email
        existing = clients_table.all(formula=f"{{{FLD_CLIENT_PRIMARY_EMAIL}}} = '{customer_email}'")
        if existing:
            logger.info("Client already exists for %s — skipping Airtable creation", customer_email)
            return

        # Determine client name: use business_name if provided, else customer_name
        client_name = business_name if business_name and business_name != "not provided" else customer_name

        today_et = datetime.now(EASTERN)
        today_str = today_et.strftime("%Y-%m-%d")
        trial_ends_str = (today_et + timedelta(days=30)).strftime("%Y-%m-%d")

        record_fields = {
            FLD_CLIENT_NAME:           client_name,
            FLD_CLIENT_PRIMARY_EMAIL:  customer_email,
            "fldIWXSLRJYAVRs3P":       customer_name,                      # Primary Contact Name
            "fldj4bxH9JwOSnGK8":       customer_email,                     # Portal Email
            "fldlB9waYE5uPj1hI":       amount,                             # Monthly Service Fee
            "flduyJEcYbWgFqpgg":       today_str,                          # Service Start Date
            "fldCL7YwtBAG7slEd":       "Active",                           # Client Status
            "flde7mzXePBqKBjGx":       "Pending — Awaiting Reply",         # Requirements Status
            # ── Subscription lifecycle (1B Stripe subscriptions work) ───
            FLD_CLIENT_SUBSCRIPTION_STATUS:  "Trial" if is_trial else "Active",
            FLD_CLIENT_SUBSCRIPTION_STARTED: today_str,
        }
        if is_trial:
            record_fields[FLD_CLIENT_TRIAL_ENDS] = trial_ends_str
        if coupon_code:
            record_fields[FLD_CLIENT_COUPON_USED] = coupon_code
        if stripe_customer_id:
            record_fields[FLD_CLIENT_STRIPE_CUSTOMER_ID] = stripe_customer_id
        if stripe_subscription_id:
            record_fields[FLD_CLIENT_STRIPE_SUBSCRIPTION_ID] = stripe_subscription_id

        # Tier + Subscription Tier kept in sync (bulk_vendor_import reads
        # Subscription Tier for cap enforcement; Tier is the new canonical name).
        # Only write if we resolved a known tier — silently allowing unbounded
        # imports would be worse than a clear hard-fail in bulk_vendor_import.
        if plan in {"Starter", "Growth", "Scale"}:
            record_fields[FLD_CLIENT_TIER] = plan
            record_fields[FLD_CLIENT_SUBSCRIPTION_TIER] = plan

        clients_table.create(record_fields, typecast=True)
        logger.info(
            "Created Airtable client record for %s (tier=%s, status=%s, coupon=%r)",
            customer_email, plan, "Trial" if is_trial else "Active", coupon_code,
        )

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


# ── Subscription lifecycle handlers ────────────────────────────────────────


def _find_client_by_stripe_subscription_id(sub_id: str):
    """Return the first Client record whose Stripe Subscription ID matches,
    or None. Safe against single-quote injection — sub_id format is sub_xxx."""
    if not sub_id:
        return None
    try:
        safe = sub_id.replace("'", "")
        formula = f"{{{FLD_CLIENT_STRIPE_SUBSCRIPTION_ID}}} = '{safe}'"
        records = _clients_table().all(formula=formula, max_records=1)
        return records[0] if records else None
    except Exception as exc:
        logger.error("Lookup failed for Stripe Subscription ID %s: %s", sub_id, exc)
        return None


def _find_client_by_stripe_customer_id(cus_id: str):
    """Return the first Client record whose Stripe Customer ID matches, or None."""
    if not cus_id:
        return None
    try:
        safe = cus_id.replace("'", "")
        formula = f"{{{FLD_CLIENT_STRIPE_CUSTOMER_ID}}} = '{safe}'"
        records = _clients_table().all(formula=formula, max_records=1)
        return records[0] if records else None
    except Exception as exc:
        logger.error("Lookup failed for Stripe Customer ID %s: %s", cus_id, exc)
        return None


def _ts_to_date_str(unix_ts):
    """Convert a Stripe Unix timestamp (or None) to YYYY-MM-DD UTC, or ""."""
    if not unix_ts:
        return ""
    try:
        return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception as exc:
        logger.warning("Could not convert Stripe timestamp %r to date: %s", unix_ts, exc)
        return ""


def _send_cancellation_email(customer_name: str, customer_email: str):
    """Send subscription-cancellation confirmation. Draft copy; replace with
    attorney-reviewed copy before launch if needed."""
    if not customer_email:
        logger.warning("No customer email — skipping cancellation confirmation")
        return
    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail, Email as SGEmail
        from legal_disclaimer import EMAIL_DISCLAIMER_HTML
        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)

        first_name = ""
        if customer_name and customer_name.strip() and customer_name.strip().lower() not in {"there", "not provided"}:
            first_name = customer_name.strip().split(" ", 1)[0]
        greeting = f"<p>Hi {first_name},</p>" if first_name else "<p>Hi,</p>"

        subject = "Your CCS subscription has ended"
        body_html = (
            greeting
            + "<p>Your Carolina Compliance Solutions subscription has been "
              "canceled. No further charges will be made.</p>"
            + "<p><strong>What happens to your data:</strong> We retain your "
              "Client and Vendor records for <strong>90 days</strong> in case "
              "you'd like to reactivate. After 90 days, your account is "
              "archived and the data is no longer accessible through the "
              "dashboard.</p>"
            + "<p>If you'd like to reactivate before then — or if you have "
              "questions about your data or your final invoice — just reply "
              "to this email and I'll take care of it.</p>"
            + "<p>Thanks for trying us out. If there's something we could "
              "have done better, I'd genuinely like to hear it.</p>"
            + "<p>Haley Bridges<br>"
              "Founder, Carolina Compliance Solutions<br>"
              f"{_cfg.OWNER_EMAIL}<br>"
              "carolinacompliancesolutions.com</p>"
            + EMAIL_DISCLAIMER_HTML
        )

        message = Mail(
            from_email=SGEmail(email=SENDER_EMAIL, name="Carolina Compliance Solutions"),
            to_emails=customer_email,
            subject=subject,
            html_content=build_email_html(subject, body_html, audience="client"),
        )
        message.reply_to = SGEmail(_cfg.reply_to_for("client"))
        sg.send(message)
        logger.info("Cancellation confirmation email sent to %s", customer_email)
    except Exception as exc:
        logger.error("Failed to send cancellation email to %s: %s", customer_email, exc)


def _handle_subscription_deleted(event):
    """customer.subscription.deleted — subscription was canceled (immediate or
    at period end). Mark Client as Canceled, record end date, send confirmation."""
    sub = event["data"]["object"]
    sub_id = sub.get("id") if hasattr(sub, "get") else getattr(sub, "id", None)
    record = _find_client_by_stripe_subscription_id(sub_id)
    if not record:
        logger.warning("subscription.deleted: no Client found for sub_id=%s", sub_id)
        return

    rec_id = record["id"]
    fields = record.get("fields", {})
    client_name = fields.get("Client Name", "(unknown)")
    customer_email = fields.get("Primary Contact Email", "")

    # Stripe sets canceled_at on user-initiated; ended_at on period-end.
    ended_ts = sub.get("canceled_at") or sub.get("ended_at") if hasattr(sub, "get") else (
        getattr(sub, "canceled_at", None) or getattr(sub, "ended_at", None)
    )
    ended_date = _ts_to_date_str(ended_ts) or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    update_fields = {
        FLD_CLIENT_SUBSCRIPTION_STATUS: "Canceled",
        FLD_CLIENT_SUBSCRIPTION_ENDED:  ended_date,
    }
    try:
        _clients_table().update(rec_id, update_fields, typecast=True)
        logger.info(
            "subscription.deleted: marked %s (%s) Canceled, ended=%s",
            client_name, sub_id, ended_date,
        )
    except Exception as exc:
        logger.error("subscription.deleted: failed to update %s: %s", rec_id, exc)

    _send_cancellation_email(client_name, customer_email)


def _handle_invoice_payment_failed(event):
    """invoice.payment_failed — mark Client Past Due (no email; Stripe dunning)."""
    invoice = event["data"]["object"]
    customer_id = invoice.get("customer") if hasattr(invoice, "get") else getattr(invoice, "customer", None)
    record = _find_client_by_stripe_customer_id(customer_id)
    if not record:
        logger.warning("invoice.payment_failed: no Client found for customer_id=%s", customer_id)
        return

    rec_id = record["id"]
    client_name = record.get("fields", {}).get("Client Name", "(unknown)")
    try:
        _clients_table().update(rec_id, {FLD_CLIENT_SUBSCRIPTION_STATUS: "Past Due"}, typecast=True)
        logger.info(
            "invoice.payment_failed: marked %s (customer=%s) Past Due",
            client_name, customer_id,
        )
    except Exception as exc:
        logger.error("invoice.payment_failed: failed to update %s: %s", rec_id, exc)


def _handle_invoice_paid(event):
    """invoice.paid — if Client was Past Due, flip back to Active. No-op otherwise."""
    invoice = event["data"]["object"]
    customer_id = invoice.get("customer") if hasattr(invoice, "get") else getattr(invoice, "customer", None)
    record = _find_client_by_stripe_customer_id(customer_id)
    if not record:
        # Common case: invoices for customers not (yet) in Airtable. Quiet log.
        logger.info("invoice.paid: no Client found for customer_id=%s — ignoring", customer_id)
        return

    rec_id = record["id"]
    fields = record.get("fields", {})
    client_name = fields.get("Client Name", "(unknown)")
    current_status = (fields.get("Subscription Status") or "").strip()

    if current_status != "Past Due":
        logger.info(
            "invoice.paid: %s status is %r (not Past Due) — no change",
            client_name, current_status,
        )
        return

    try:
        _clients_table().update(rec_id, {FLD_CLIENT_SUBSCRIPTION_STATUS: "Active"}, typecast=True)
        logger.info("invoice.paid: %s flipped Past Due -> Active", client_name)
    except Exception as exc:
        logger.error("invoice.paid: failed to update %s: %s", rec_id, exc)


def _handle_subscription_updated(event):
    """customer.subscription.updated — many possible triggers. We only act on:

    1. cancel_at_period_end transitioning False -> True (log only, no write)
    2. Price/plan change to a different tier (write Tier + Subscription Tier)
    3. Coupon expired post-trial: previous_attributes had discount, current
       object has none → flip Subscription Status Trial -> Active.
    """
    sub = event["data"]["object"]
    prev = event["data"].get("previous_attributes", {}) or {}
    sub_id = sub.get("id") if hasattr(sub, "get") else getattr(sub, "id", None)
    record = _find_client_by_stripe_subscription_id(sub_id)
    if not record:
        logger.info("subscription.updated: no Client found for sub_id=%s — ignoring", sub_id)
        return

    rec_id = record["id"]
    fields = record.get("fields", {})
    client_name = fields.get("Client Name", "(unknown)")
    current_tier = (fields.get("Tier") or "").strip()
    current_status = (fields.get("Subscription Status") or "").strip()

    # ── 1. cancel_at_period_end False -> True: log only ─────────────────────
    if "cancel_at_period_end" in prev:
        was = prev.get("cancel_at_period_end")
        now = sub.get("cancel_at_period_end") if hasattr(sub, "get") else getattr(sub, "cancel_at_period_end", None)
        if was is False and now is True:
            logger.info(
                "subscription.updated: %s scheduled cancellation at period end (sub=%s) — no status change yet",
                client_name, sub_id,
            )
            # Continue — there may be other updates to process in the same event

    # ── 2. Tier change ──────────────────────────────────────────────────────
    # Get current price id from items.data[0].price.id (single-item subs).
    new_price_id = ""
    try:
        items = sub.get("items") if hasattr(sub, "get") else getattr(sub, "items", None)
        items_data = items.get("data") if items and hasattr(items, "get") else getattr(items, "data", None) if items else None
        if items_data and len(items_data) > 0:
            first_item = items_data[0]
            price_obj = first_item.get("price") if hasattr(first_item, "get") else getattr(first_item, "price", None)
            if price_obj:
                new_price_id = price_obj.get("id") if hasattr(price_obj, "get") else getattr(price_obj, "id", "") or ""
    except Exception as exc:
        logger.warning("subscription.updated: could not extract new price ID: %s", exc)

    new_tier = _resolve_tier(price_id=new_price_id)
    if new_tier and new_tier != current_tier:
        try:
            _clients_table().update(rec_id, {
                FLD_CLIENT_TIER:              new_tier,
                FLD_CLIENT_SUBSCRIPTION_TIER: new_tier,
            }, typecast=True)
            logger.info(
                "subscription.updated: %s tier changed %r -> %r (sub=%s)",
                client_name, current_tier, new_tier, sub_id,
            )
        except Exception as exc:
            logger.error("subscription.updated: failed to update tier for %s: %s", rec_id, exc)

    # ── 3. Coupon expired (post-trial conversion) ──────────────────────────
    # Detect by previous_attributes containing 'discount' (i.e. it changed)
    # AND current object having no discount → coupon was removed.
    if "discount" in prev:
        had_discount = prev.get("discount") is not None
        now_discount = sub.get("discount") if hasattr(sub, "get") else getattr(sub, "discount", None)
        if had_discount and now_discount is None and current_status == "Trial":
            try:
                _clients_table().update(rec_id, {FLD_CLIENT_SUBSCRIPTION_STATUS: "Active"}, typecast=True)
                logger.info(
                    "subscription.updated: %s coupon expired, flipped Trial -> Active (sub=%s)",
                    client_name, sub_id,
                )
            except Exception as exc:
                logger.error("subscription.updated: failed to flip Trial->Active for %s: %s", rec_id, exc)


if __name__ == "__main__":
    logger.info("=== Module 16: Stripe Webhook Service starting ===")
    logger.info("Business hours: Mon-Fri %dam-%dpm ET", BUSINESS_HOURS_START, BUSINESS_HOURS_END)
    port = int(os.environ.get("PORT", 5051))
    app.run(host="0.0.0.0", port=port, debug=False)
