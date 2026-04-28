"""
Module 16 — Inbound COI Webhook
Receives COI PDFs from SendGrid Inbound Parse.
During business hours (8am-6pm ET Mon-Fri): runs full pipeline immediately.
Outside business hours: saves to queue for 8am cron batch.
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

UPLOAD_DIR = Path(__file__).parent / "incoming_pdfs"
UPLOAD_DIR.mkdir(exist_ok=True)

SENDGRID_WEBHOOK_SECRET = os.getenv("SENDGRID_WEBHOOK_SECRET", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
import config as _cfg
OWNER_EMAIL = _cfg.OWNER_EMAIL
SENDER_EMAIL = _cfg.FROM_EMAIL


def is_business_hours():
    # Delegates to module_21_business_hours (shared source of truth).
    return _is_business_hours_impl()


def save_pdf(filename, data):
    safe_name = "".join(c for c in filename if c.isalnum() or c in "._- ").strip()
    if not safe_name.lower().endswith(".pdf"):
        safe_name += ".pdf"
    dest = UPLOAD_DIR / safe_name
    # avoid collisions
    counter = 1
    while dest.exists():
        stem = Path(safe_name).stem
        dest = UPLOAD_DIR / f"{stem}_{counter}.pdf"
        counter += 1
    dest.write_bytes(data)
    logger.info("Saved PDF: %s", dest)
    return dest


def run_pipeline(pdf_path):
    import subprocess
    python = Path(__file__).parent / ".venv" / "bin" / "python"
    scripts = [
        "module_2_extractor.py",
        "module_3_airtable_importer.py",
        "module_4_policy_processor.py",
        "module_7b_requirement_validator.py",
        "module_8_expiration_checker.py",
        "module_8b.py",
        "module_15_email_queue_builder.py",
        "module_10_sendgrid_sender.py",
    ]
    env = os.environ.copy()
    env["COI_TARGET_FILE"] = str(pdf_path)

    for script in scripts:
        script_path = Path(__file__).parent / script
        if not script_path.exists():
            logger.warning("Script not found, skipping: %s", script)
            continue
        logger.info("Running %s", script)
        result = subprocess.run(
            [str(python), str(script_path)],
            capture_output=True, text=True, env=env
        )
        if result.returncode != 0:
            logger.error("Error in %s: %s", script, result.stderr[-500:])
            break
        else:
            logger.info("Completed %s", script)


@app.route("/webhook/inbound-coi", methods=["POST"])
def inbound_coi():
    sender = request.form.get("from", "unknown")
    subject = request.form.get("subject", "")
    logger.info("Inbound email from: %s | Subject: %s", sender, subject)

    attachments = int(request.form.get("attachments", 0))
    if attachments == 0:
        logger.warning("No attachments found in email from %s", sender)
        return jsonify({"status": "ignored", "reason": "no attachments"}), 200

    saved_pdfs = []
    for i in range(1, attachments + 1):
        attachment = request.files.get(f"attachment{i}")
        if attachment is None:
            continue
        filename = attachment.filename or f"coi_{i}.pdf"
        if not filename.lower().endswith(".pdf"):
            logger.info("Skipping non-PDF attachment: %s", filename)
            continue
        data = attachment.read()
        pdf_path = save_pdf(filename, data)
        saved_pdfs.append(pdf_path)

    if not saved_pdfs:
        return jsonify({"status": "ignored", "reason": "no PDF attachments"}), 200

    if is_business_hours():
        logger.info("Business hours — running pipeline immediately for %d PDFs", len(saved_pdfs))
        for pdf_path in saved_pdfs:
            run_pipeline(pdf_path)
        return jsonify({"status": "processed", "files": [str(p) for p in saved_pdfs]}), 200
    else:
        logger.info("Outside business hours — %d PDFs queued for morning batch", len(saved_pdfs))
        return jsonify({"status": "queued", "files": [str(p) for p in saved_pdfs]}), 200


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
            # Update these Price IDs from your Stripe dashboard
            "price_starter": "Starter",
            "price_growth": "Growth",
            "price_pro": "Pro",
            "price_scale": "Scale",
        }
        price_id = session.get("line_items", {}).get("data", [{}])[0].get("price", {}).get("id", "")
        plan = price_to_plan.get(price_id)
        if not plan:
            plan = {14900: "Starter", 39900: "Growth", 59900: "Pro", 79900: "Scale"}.get(session.amount_total, "Unknown Plan")
            logger.warning("Plan detection fell back to amount-based matching. Amount: %s, Resolved: %s", session.amount_total, plan)

        amount = session.amount_total / 100 if session.amount_total else 0

        logger.info("New customer: %s (%s) — %s — $%.2f/mo", customer_name, customer_email, plan, amount)
        _send_owner_notification(customer_name, customer_email, customer_phone, business_name, plan, amount)
        _create_airtable_client(customer_name, customer_email, business_name, amount)
        _send_welcome_email(customer_name, customer_email)
        _send_onboarding_email(customer_name, customer_email)

    return jsonify({"status": "ok"}), 200


def _create_airtable_client(customer_name, customer_email, business_name, amount):
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

        clients_table.create(record_fields)
        logger.info("Created Airtable client record for %s", customer_email)

    except Exception as e:
        logger.error("Failed to create Airtable client record for %s: %s", customer_email, e)


def _send_owner_notification(name, email, phone, business, plan, amount):
    import sendgrid
    from sendgrid.helpers.mail import Mail
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


def _send_welcome_email(name, email):
    # Idempotency check: skip if welcome email already sent (Airtable lookup)
    if _has_welcome_been_sent_recently(email):
        logger.info("Welcome email already sent to %s — skipping.", email)
        return

    import sendgrid
    from sendgrid.helpers.mail import Mail
    from legal_disclaimer import EMAIL_DISCLAIMER_HTML
    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)

    welcome_subject = "You're in. Let's get your subs in line. 🏗️"
    welcome_body_html = (
        f"<p>Hi {name},</p>"
        "<p>Welcome to Carolina Compliance Solutions — and congratulations on "
        "officially retiring your COI spreadsheet. It had a good run. "
        "We'll take it from here.</p>"
        "<p>Here's what happens next. Four steps. That's it.</p>"
        "<p><strong>Step 1 — Send us your vendor list</strong><br>"
        "Just reply to this email with your subcontractors — however you have "
        "them. A list, a spreadsheet, a napkin photo. We'll get them loaded in.</p>"
        "<p><strong>Step 2 — Forward your existing COIs</strong><br>"
        "Got certificates already on file? Forward them to "
        f"{_cfg.INBOUND_EMAIL}. Our system reads and tracks them "
        "automatically.</p>"
        "<p><strong>Step 3 — Log into your dashboard</strong><br>"
        'Head to <a href="https://app.carolinacompliancesolutions.com">'
        "app.carolinacompliancesolutions.com</a>. Your compliance dashboard is ready.</p>"
        "<p><strong>Step 4 — Tell us your requirements</strong><br>"
        "Reply to this email with the insurance minimums you require from your "
        "subcontractors:</p>"
        "<ul>"
        "<li>General Liability (GL) — required? If yes, minimum limit?</li>"
        "<li>Auto Liability (AL) — required? If yes, minimum limit?</li>"
        "<li>Workers Compensation (WC) — required yes or no?</li>"
        "<li>Additional Insured (AI) — required yes or no?</li>"
        "<li>Waiver of Subrogation (WOS) — required yes or no?</li>"
        "</ul>"
        "<p>We'll load your requirements into the system within 1 business day.</p>"
        "<p>That's it. No more chasing your electrician for his certificate. "
        "No more wondering if your framer's workers' comp lapsed mid-project. "
        "We track it so you don't have to.</p>"
        "<p>Reply to this email anytime with questions.</p>"
        "<p>Welcome aboard,<br>"
        "<strong>The Carolina Compliance Team</strong><br>"
        "carolinacompliancesolutions.com</p>"
        "<p><em>P.S. Your electrician already needs a reminder. We're on it.</em></p>"
        f"{EMAIL_DISCLAIMER_HTML}"
    )

    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=email,
        subject=welcome_subject,
        html_content=build_email_html(welcome_subject, welcome_body_html),
    )
    sg.send(message)
    _record_welcome_email_sent(email)
    logger.info("Welcome email sent to %s", email)


def _send_onboarding_email(name, email):
    """Send the 'what happens next' onboarding email immediately after welcome."""
    import sendgrid
    from sendgrid.helpers.mail import Mail, Email as SGEmail
    from legal_disclaimer import EMAIL_DISCLAIMER_HTML

    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)

    onboarding_subject = "What happens next — Carolina Compliance Solutions"
    onboarding_body_html = (
        f"<p>Hi {name},</p>"
        "<p>Welcome to Carolina Compliance Solutions. Here's what happens next:</p>"
        "<p><strong>1.</strong> We'll reach out within 24 hours to collect your "
        "subcontractor insurance requirements — things like coverage types, minimum "
        "limits, and any endorsements your contracts require.</p>"
        "<p><strong>2.</strong> Once we have your requirements confirmed, we'll contact "
        "your subcontractors automatically and start collecting their certificates.</p>"
        "<p><strong>3.</strong> You can log into your compliance portal any time to see "
        "the status of every subcontractor:<br>"
        '<a href="https://app.carolinacompliancesolutions.com">'
        "app.carolinacompliancesolutions.com</a></p>"
        "<p>If you have questions in the meantime, just reply to this email.</p>"
        "<p>Carolina Compliance Solutions<br>"
        f"{_cfg.INBOUND_EMAIL}</p>"
        f"{EMAIL_DISCLAIMER_HTML}"
    )

    message = Mail(
        from_email=SGEmail(email=SENDER_EMAIL, name="Carolina Compliance Solutions"),
        to_emails=email,
        subject=onboarding_subject,
        html_content=build_email_html(onboarding_subject, onboarding_body_html),
    )
    message.reply_to = SGEmail(_cfg.INBOUND_EMAIL)

    try:
        sg.send(message)
        logger.info("Onboarding email sent to %s", email)
    except Exception as e:
        logger.error("Failed to send onboarding email to %s: %s", email, e)


if __name__ == "__main__":
    logger.info("=== Module 16: Inbound COI Webhook starting ===")
    logger.info("PDF drop folder: %s", UPLOAD_DIR)
    logger.info("Business hours: Mon-Fri %dam-%dpm ET", BUSINESS_HOURS_START, BUSINESS_HOURS_END)
    port = int(os.environ.get("PORT", 5051))
    app.run(host="0.0.0.0", port=port, debug=False)
