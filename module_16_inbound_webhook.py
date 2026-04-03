"""
Module 16 — Inbound COI Webhook
Receives COI PDFs from SendGrid Inbound Parse.
During business hours (8am-6pm ET Mon-Fri): runs full pipeline immediately.
Outside business hours: saves to queue for 8am cron batch.
"""

import os
import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

UPLOAD_DIR = Path(__file__).parent / "incoming_pdfs"
UPLOAD_DIR.mkdir(exist_ok=True)

EASTERN = ZoneInfo("America/New_York")
BUSINESS_HOURS_START = 8   # 8am ET
BUSINESS_HOURS_END = 18    # 6pm ET
BUSINESS_DAYS = {0, 1, 2, 3, 4}  # Mon-Fri

SENDGRID_WEBHOOK_SECRET = os.getenv("SENDGRID_WEBHOOK_SECRET", "")


def is_business_hours():
    now = datetime.now(EASTERN)
    return (
        now.weekday() in BUSINESS_DAYS
        and BUSINESS_HOURS_START <= now.hour < BUSINESS_HOURS_END
    )


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


if __name__ == "__main__":
    logger.info("=== Module 16: Inbound COI Webhook starting ===")
    logger.info("PDF drop folder: %s", UPLOAD_DIR)
    logger.info("Business hours: Mon-Fri %dam-%dpm ET", BUSINESS_HOURS_START, BUSINESS_HOURS_END)
    app.run(host="0.0.0.0", port=5051, debug=False)
