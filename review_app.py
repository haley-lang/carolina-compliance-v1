"""Flask review app for the Incoming Extractions queue.

Start:
    REVIEW_PASSWORD=secret .venv/bin/python review_app.py

Requires REVIEW_PASSWORD to be set. Reads the same AIRTABLE_API_KEY /
AIRTABLE_BASE_ID as the rest of the pipeline from .env.

For production deploy (Railway) also set:
    SESSION_SECRET_KEY — random 32+ byte hex string
    FLASK_HTTPS=1      — enables Secure cookie flag

What Approve triggers downstream:
    Approve changes Review Status to "Approved" or "Approved + Edited".
    It does NOT change Processing Status, so processor.py (which queries
    Processing Status = 'Imported') will NOT pick the record up for
    certificate/policy creation. The record is approved for audit purposes
    but will only re-enter the pipeline if Processing Status is manually
    set back to "Imported" or re-extracted.
"""
import csv
import io
import json
import os
import re
import secrets
import subprocess
import threading
import time
import traceback
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from functools import wraps

from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, abort, flash, redirect, render_template,
    request, session, url_for,
)

import config
import health_store as _hs
from airtable_importer import clean_base_id, INCOMING_EXTRACTIONS_TABLE
from review_gate import (
    REVIEW_STATUS_APPROVED, REVIEW_STATUS_APPROVED_EDITED,
    REVIEW_STATUS_REJECTED, REVIEW_STATUS_ESCALATED,
    REVIEW_STATUS_PENDING_REVIEW, REVIEW_STATUS_AUTO_APPROVED,
)

# ── Constants ────────────────────────────────────────────────────────────────

CORRECTIONS_TABLE_ID       = "tblh6NTQikjzl8FFu"
TRAINING_LOG_TABLE_ID      = "tblBW6HkbSVYkN2PN"  # Extraction Training Log (test set)
EXTRACTIONS_TABLE_ID       = "tblT88Ty6d6M766oY"  # Incoming Extractions — for Airtable deep-links
INSURANCE_POLICIES_TABLE_ID = "tblpPcmm5ANE0bMNB"  # Insurance Policies
ACCEPTED_POLICY_PATTERNS_TABLE_ID = "tblnrj457WdtRhkei"
GAP_REQUESTS_TABLE_ID      = "tbleY5UzcLYZFZFKF"
AUDITS_TABLE_ID            = "tblbaTMKuM6yWeVjL"
SUB_PAYMENTS_TABLE_ID      = "tbleeHHzDSZ2bpwd0"
VENDORS_TABLE_ID           = "tblsOphSd5DKSZEro"
CLIENTS_TABLE_ID           = "tbltnBIWke20IEI3K"
VCA_TABLE_ID               = "tblpYKywfs0YHiQ98"
EMAIL_MESSAGES_TABLE_ID    = "tblE5WKGwLF6eHLwA"
RTR_CLIENT_ID              = "recG2hxGoJKdgBr4M"

_THREAD_KEY_ENABLED = os.environ.get("THREAD_KEY_ENABLED", "1").lower() not in ("0", "false", "no")

_BILLY_RTR_PREFIXES = ("COI_forms_", "RTR_COI_", "N9WC", "coi.json")

# Accepted certificate holder aliases for RTR Construction (normalised lowercase, no punctuation)
_RTR_CLIENT_ALIASES = [
    "rtr construction llc", "rtr renovations llc", "rtr renovation llc",
    "rtr construction", "rtr renovations", "rtr renovation",
    "rtr", "renovations to roofing",
]

def _norm_holder(s: str) -> str:
    """Lowercase, strip all non-alphanumeric except spaces."""
    return re.sub(r"[^a-z0-9 ]", "", (s or "").lower()).strip()


def _is_billy_rtr(source_filename: str) -> bool:
    """True if the filename belongs to the Billy/RTR real-client batch."""
    fn = source_filename or ""
    return (
        any(fn.startswith(p) for p in _BILLY_RTR_PREFIXES)
        or "resend" in fn.lower()
    )


def _norm_ni(name: str) -> str:
    """Normalize a Named Insured for duplicate detection."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", (name or "").lower())).strip()


def _norm_polnum(v: str) -> str:
    """Normalize a policy number for duplicate matching (strip punctuation/spaces)."""
    return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())

_GL_KEYWORDS = ("general", "liability", "cgl", "commercial general")
_WC_KEYWORDS = ("workers", "work comp", "w/c", "wc", "workers'", "employers liability")


def _pol_matches_audit_type(pol_type: str, kws: tuple) -> bool:
    """True if pol_type matches the keywords.

    For GL matching, explicitly excludes 'auto' and 'employers' so that
    'Auto Liability', 'Commercial Auto Liability', and 'Employers Liability'
    are not incorrectly classified as General Liability.
    """
    pt = pol_type.lower()
    if not any(k in pt for k in kws):
        return False
    if kws is _GL_KEYWORDS and ("auto" in pt or "employers" in pt):
        return False
    return True


# Required policy types for RTR Construction (no Airtable requirement rows → default to GL + WC)
RTR_REQUIRED_POLICY_TYPES = [
    (_GL_KEYWORDS, "General Liability"),
    (_WC_KEYWORDS, "Workers' Comp"),
]

# AI disagreement field keywords that are annotation facts, not substantive required-field values.
# A disagreement line containing ONLY these words is not a "needs decision" trigger.
_ANNOTATION_DISAGREE_KEYWORDS = (
    "wos", "waiver_of_subrogation", "additional_insured", "pnc",
    "primary_noncontributory", "statement",
)


def _required_field_ai_disagreed(ai_raw: str) -> bool:
    """True if any AI disagreement line involves a substantive field (not AI/WoS/PNC)."""
    lines = [ln.strip() for ln in str(ai_raw or "").split("\n") if ln.strip()]
    for line in lines:
        ll = line.lower()
        if not any(k in ll for k in _ANNOTATION_DISAGREE_KEYWORDS):
            return True
    return False

def _find_key_policy(policies: list, keywords: tuple) -> tuple:
    """Return (index, policy_dict) for the first policy whose type matches any keyword."""
    for i, p in enumerate(policies):
        pt = (p.get("policy_type") or "").lower()
        if any(k in pt for k in keywords):
            return i, p
    return None, None


def _get_required_indices(policies: list, required_policy_types: list) -> set:
    """Return set of policy indices matching any required type."""
    indices = set()
    for keywords, _ in required_policy_types:
        idx, _ = _find_key_policy(policies, keywords)
        if idx is not None:
            indices.add(idx)
    return indices


def _split_issues(issues: list, required_indices: set) -> tuple:
    """Return (primary, secondary) where primary = issues on required types, NI, or doc type."""
    primary_exact = {"named_insured", "document_type"}
    required_prefixes = {f"policy_{i}_" for i in required_indices}
    primary, secondary = [], []
    for issue in issues:
        field = issue.get("field") or ""
        is_primary = (
            field in primary_exact
            or issue.get("type") in ("non_coi", "possible_duplicate", "expired_arrival", "surplus_lines")
            or issue.get("id") in ("ai_named_insured", "possible_duplicate")
            or "missing_required" in (issue.get("id") or "")
            or "expired_arrival" in (issue.get("id") or "")
            or any(field.startswith(pfx) for pfx in required_prefixes)
        )
        (primary if is_primary else secondary).append(issue)
    return primary, secondary


SECOND_READ_JSON_FIELD_ID = "fldJFX9EN6j40cdXD"
AI_DISAGREEMENTS_FIELD_ID = "fldbiX9zZHdF3RGDQ"
PRIOR_RAW_JSON_FIELD_NAME = "Prior Raw JSON (superseded)"
SOURCE_PAGE_FIELD = "Source Page"

SESSION_TIMEOUT_SECONDS = 3600       # 60-minute idle timeout
_RATE_LIMIT_MAX_FAILURES = 5         # failed attempts before lockout
_RATE_LIMIT_WINDOW_SECONDS = 900     # 15-minute rolling window
_RATE_LIMIT_LOCKOUT_SECONDS = 900    # 15-minute lockout duration

# Per-process login failure tracking: IP → {"failures": [timestamps], "locked_until": float}
_login_failures: dict = {}

try:
    from extractor import EXTRACTION_MODEL
except Exception:
    EXTRACTION_MODEL = "unknown"

# ── App setup ────────────────────────────────────────────────────────────────

REVIEW_PASSWORD = os.environ.get("REVIEW_PASSWORD", "").strip()
if not REVIEW_PASSWORD:
    raise EnvironmentError(
        "REVIEW_PASSWORD is not set. "
        "Run: REVIEW_PASSWORD=yourpassword .venv/bin/python review_app.py"
    )

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET_KEY") or (REVIEW_PASSWORD + "_review_secret_v1")
app.jinja_env.globals["enumerate"] = enumerate

# Reverse proxy / custom domain support — Railway terminates TLS and forwards HTTP.
# ProxyFix makes url_for() generate correct https:// URLs behind Railway's proxy.
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

_https_mode = os.environ.get("FLASK_HTTPS", "").lower() in ("1", "true", "yes")
app.config["SESSION_COOKIE_SECURE"] = _https_mode
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

# Optional: scope session cookie to a specific (sub)domain, e.g. ".review.example.com"
_cookie_domain = os.environ.get("SESSION_COOKIE_DOMAIN", "").strip()
if _cookie_domain:
    app.config["SESSION_COOKIE_DOMAIN"] = _cookie_domain


# ── CSRF ─────────────────────────────────────────────────────────────────────

def _csrf_token() -> str:
    """Return (and create if needed) the per-session CSRF token."""
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]

app.jinja_env.globals["csrf_token"] = _csrf_token

_DISPLAY_STATUS_MAP = {
    REVIEW_STATUS_APPROVED:         "Data confirmed",
    REVIEW_STATUS_APPROVED_EDITED:  "Data confirmed",
    REVIEW_STATUS_AUTO_APPROVED:    "Data confirmed (auto)",
}

def _display_review_status(status: str) -> str:
    return _DISPLAY_STATUS_MAP.get(status, status)

app.jinja_env.globals["display_review_status"] = _display_review_status


def _norm_tristate(v) -> str:
    """Normalize a possibly-boolean checkbox to tri-state string. Back-compat: bool True → 'checked'."""
    if isinstance(v, bool):
        return "checked" if v else "not_checked"
    if v in ("checked", "not_checked", "not_applicable"):
        return v
    return "not_checked"

app.jinja_env.globals["norm_tristate"] = _norm_tristate


@app.context_processor
def _inject_email_badge():
    """Inject email draft count into every template for the nav badge."""
    def _email_draft_count():
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            msgs = _em_table().all(fields=["Status", "Snoozed Until"])
            return sum(
                1 for m in msgs
                if (m["fields"].get("Status") or "") == "Draft"
                and not bool((m["fields"].get("Snoozed Until") or "") > now_iso)
            )
        except Exception:
            return 0
    return {"email_draft_count": _email_draft_count}


@app.context_processor
def _inject_pulse_banner():
    """Inject overall health level and banner text into every template."""
    def _banner():
        try:
            events = _hs.get_recent_events(limit=20, since_hours=1)
            errors  = [e for e in events if e.get("level") == "error"]
            warns   = [e for e in events if e.get("level") == "warn"]
            if errors:
                return {"level": "error", "text": errors[0]["title"]}
            if warns:
                return {"level": "warn",  "text": warns[0]["title"]}
        except Exception:
            pass
        return {"level": "ok", "text": ""}
    return {"pulse_banner": _banner}


def _fmt_date(val):
    """Convert YYYY-MM-DD to MM/DD/YYYY for display. Passes anything else through unchanged."""
    if not val:
        return val
    s = str(val).strip()
    try:
        from datetime import datetime as _dt
        return _dt.strptime(s, "%Y-%m-%d").strftime("%m/%d/%Y")
    except ValueError:
        return s

app.jinja_env.filters["fmt_date"] = _fmt_date


def _fmt_received(iso_str: str) -> str:
    """Format an ISO-8601 date/datetime as 'Nov 3, 1:41 PM' for display."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        # Convert to ET for display
        from zoneinfo import ZoneInfo
        dt = dt.astimezone(ZoneInfo("America/New_York"))
        # e.g. "Nov 3, 1:41 PM"
        return dt.strftime("%-m/%-d, %-I:%M %p").replace("AM", "am").replace("PM", "pm")
    except Exception:
        return iso_str[:10]

app.jinja_env.filters["fmt_received"] = _fmt_received


def _timestamp_fmt(ts) -> str:
    """Format a Unix timestamp as MM/DD/YYYY."""
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%-m/%-d/%Y")
    except Exception:
        return str(ts)

app.jinja_env.filters["timestamp_fmt"] = _timestamp_fmt


def _age_tag(iso_str: str) -> dict:
    """Return {'label': '2 days', 'level': 'neutral'|'amber'|'red'} for a received timestamp."""
    if not iso_str:
        return {"label": "", "level": "neutral"}
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        days = (datetime.now(timezone.utc) - dt).days
        if days == 0:
            label = "today"
        elif days == 1:
            label = "1 day"
        else:
            label = f"{days} days"
        level = "red" if days >= 7 else ("amber" if days >= 3 else "neutral")
        return {"label": label, "level": level}
    except Exception:
        return {"label": "", "level": "neutral"}

app.jinja_env.globals["age_tag"] = _age_tag


def _fmt_received_long(iso_str: str) -> str:
    """Format as 'Nov 3 (2 days ago)' for the detail header."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        from zoneinfo import ZoneInfo
        dt = dt.astimezone(ZoneInfo("America/New_York"))
        days = (datetime.now(timezone.utc) - datetime.fromisoformat(iso_str.replace("Z", "+00:00"))).days
        date_part = dt.strftime("%b %-d")
        if days == 0:
            ago = "today"
        elif days == 1:
            ago = "1 day ago"
        else:
            ago = f"{days} days ago"
        return f"{date_part} ({ago})"
    except Exception:
        return iso_str[:10]

app.jinja_env.filters["fmt_received_long"] = _fmt_received_long


def csrf_protect(f):
    """Decorator: reject POST requests that don't carry the correct session CSRF token.

    Checks both form data and JSON body (for JSON API endpoints).
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        if request.method == "POST":
            submitted = request.form.get("csrf_token", "")
            if not submitted:
                # Try JSON body
                try:
                    jdata = request.get_json(silent=True, force=True) or {}
                    submitted = jdata.get("csrf_token", "")
                except Exception:
                    submitted = ""
            expected = session.get("csrf_token", "")
            if not submitted or not expected or not secrets.compare_digest(submitted, expected):
                abort(403)
        return f(*args, **kwargs)
    return wrapper


# ── Rate limiter ─────────────────────────────────────────────────────────────

def _client_ip() -> str:
    return request.remote_addr or "unknown"


def _is_rate_limited(ip: str) -> bool:
    now = time.time()
    entry = _login_failures.get(ip, {"failures": [], "locked_until": 0.0})
    if now < entry["locked_until"]:
        return True
    entry["failures"] = [t for t in entry["failures"] if now - t < _RATE_LIMIT_WINDOW_SECONDS]
    _login_failures[ip] = entry
    return False


def _record_login_failure(ip: str) -> None:
    now = time.time()
    entry = _login_failures.get(ip, {"failures": [], "locked_until": 0.0})
    entry["failures"].append(now)
    entry["failures"] = [t for t in entry["failures"] if now - t < _RATE_LIMIT_WINDOW_SECONDS]
    if len(entry["failures"]) >= _RATE_LIMIT_MAX_FAILURES:
        entry["locked_until"] = now + _RATE_LIMIT_LOCKOUT_SECONDS
    _login_failures[ip] = entry


def _clear_login_failures(ip: str) -> None:
    _login_failures.pop(ip, None)


# ── Auth ─────────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login_page"))
        logged_in_at = session.get("logged_in_at", 0)
        if time.time() - logged_in_at > SESSION_TIMEOUT_SECONDS:
            session.clear()
            flash("Session expired. Please log in again.", "warning")
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return wrapper


@app.route("/login", methods=["GET", "POST"])
@csrf_protect
def login_page():
    if session.get("authenticated"):
        return redirect(url_for("queue"))
    if request.method == "POST":
        ip = _client_ip()
        if _is_rate_limited(ip):
            flash("Too many failed attempts. Try again in 15 minutes.", "danger")
            return render_template("login.html"), 429
        if request.form.get("password") == REVIEW_PASSWORD:
            _clear_login_failures(ip)
            session["authenticated"] = True
            session["logged_in_at"] = time.time()
            return redirect(url_for("queue"))
        _record_login_failure(ip)
        flash("Wrong password.", "danger")
    return render_template("login.html")


# ── Error handlers ────────────────────────────────────────────────────────────

@app.errorhandler(403)
def forbidden(e):
    return render_template("error.html",
        code=403,
        title="You don't have permission to see this.",
        message="If you think that's wrong, try signing in again."), 403


@app.errorhandler(404)
def not_found(e):
    return render_template("error.html",
        code=404,
        title="We couldn't find that page.",
        message="It may have moved, or the link might be out of date."), 404


@app.errorhandler(500)
def server_error(e):
    app.logger.error("500 error:\n%s", traceback.format_exc())
    return render_template("error.html",
        code=500,
        title="Something went wrong on our end.",
        message="Try refreshing — if it keeps happening, let Haley know."), 500


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


# ── Airtable helpers ─────────────────────────────────────────────────────────

def _api():
    from pyairtable import Api
    return Api((config.AIRTABLE_API_KEY or "").strip())


def _ie_table():
    return _api().table(clean_base_id(config.AIRTABLE_BASE_ID), INCOMING_EXTRACTIONS_TABLE)


def _doc_table():
    return _api().table(clean_base_id(config.AIRTABLE_BASE_ID), "Incoming Documents")


def _received_at_map() -> dict:
    """Return {extraction_json_filename: date_received_iso_str} from Incoming Documents.

    File Names stores comma-separated upload paths like 'uploads/some_coi.pdf'.
    The extractor writes the JSON with the same stem: 'some_coi.json'.
    """
    from pathlib import Path as _Path
    result: dict = {}
    try:
        docs = _doc_table().all(fields=["Date Received", "File Names"])
        for rec in docs:
            f = rec["fields"]
            date_received = (f.get("Date Received") or "").strip()
            if not date_received:
                continue
            for raw_path in (f.get("File Names") or "").split(","):
                stem = _Path(raw_path.strip()).stem
                if stem:
                    result[stem + ".json"] = date_received
    except Exception as exc:
        app.logger.warning("_received_at_map failed: %s", exc)
    return result


def _em_table():
    return _api().table(clean_base_id(config.AIRTABLE_BASE_ID), EMAIL_MESSAGES_TABLE_ID)


def _gen_thread_key() -> str:
    """Generate a unique [CCS-XXXX] thread key for outbound email subjects."""
    return "CCS-" + secrets.token_hex(2).upper()


def _create_email_message(
    subject: str,
    body: str,
    to_email: str,
    cc_email: str = "",
    to_source: str = "",
    items_covered: str = "",
    vendor_ids: list = None,
    client_ids: list = None,
    gap_request_ids: list = None,
    extraction_ids: list = None,
    now_iso: str = None,
) -> dict:
    """Create a Draft record in the Email Messages table. Returns the record."""
    if now_iso is None:
        now_iso = datetime.now(timezone.utc).isoformat()
    thread_key = _gen_thread_key() if _THREAD_KEY_ENABLED else ""
    full_subject = f"{subject} [{thread_key}]" if thread_key else subject
    fields: dict = {
        "Subject":       full_subject,
        "Direction":     "Draft",
        "Status":        "Draft",
        "To":            to_email,
        "CC":            cc_email,
        "Body":          body,
        "Original Body": body,
        "Thread Key":    thread_key,
        "To Source":     to_source,
        "Items Covered": items_covered,
        "Edited At":     now_iso,
    }
    if vendor_ids:
        fields["Vendor"] = vendor_ids
    if client_ids:
        fields["Client"] = client_ids
    if gap_request_ids:
        fields["Linked Gap Request"] = gap_request_ids
    if extraction_ids:
        fields["Linked Certificate"] = extraction_ids
    try:
        return _em_table().create(fields)
    except Exception as exc:
        app.logger.error("_create_email_message failed: %s", exc)
        return {}


def _corrections_table():
    return _api().table(clean_base_id(config.AIRTABLE_BASE_ID), CORRECTIONS_TABLE_ID)


def _training_log_table():
    return _api().table(clean_base_id(config.AIRTABLE_BASE_ID), TRAINING_LOG_TABLE_ID)


def _trigger_post_confirm(record_id: str) -> None:
    """Create vendor/VCA, set Processing Status = Imported, then run processor.py in background.

    Runs synchronously for the Airtable steps (fast), then fires processor.py
    as a detached subprocess so the HTTP response isn't blocked.
    Called from _handle_approve for every RTR confirmation.
    """
    try:
        from post_confirm_processor import post_confirm_single
        from pyairtable import Api as _PApi
        _key = (config.AIRTABLE_API_KEY or "").strip()
        _bid = clean_base_id(config.AIRTABLE_BASE_ID)
        _api_inst = _PApi(_key)
        result = post_confirm_single(_api_inst, _bid, record_id, create_possible=True)
        action = result.get("action", "")
        if action == "error":
            app.logger.error("post_confirm error for %s: %s", record_id, result.get("error"))
            return
        if action == "skip":
            app.logger.info("post_confirm skip for %s: %s", record_id, result.get("reason"))
            return
        if action == "possible_match":
            app.logger.warning(
                "post_confirm possible_match for %s: '%s' ~ '%s' (%s) — created as new vendor",
                record_id, result.get("named_insured"), result.get("vendor_name"), result.get("note"),
            )
            return
        app.logger.info(
            "post_confirm %s: vendor=%s (%s) vca=%s vca_new=%s",
            action, result.get("vendor_name"), result.get("vendor_id"),
            result.get("vca_id"), result.get("vca_created"),
        )
        # Fire processor.py as a daemon thread so it runs after this request returns
        _script = str(os.path.join(os.path.dirname(__file__), ".venv", "bin", "python"))
        _proc_py = str(os.path.join(os.path.dirname(__file__), "processor.py"))
        vendor_id = result.get("vendor_id")
        def _run():
            try:
                subprocess.run([_script, _proc_py], cwd=os.path.dirname(__file__), timeout=300)
            except Exception as exc:
                app.logger.warning("processor.py subprocess error: %s", exc)
            if vendor_id:
                try:
                    _check_shared_policy_numbers(vendor_id, record_id)
                except Exception as exc:
                    app.logger.warning("_check_shared_policy_numbers failed for %s: %s", vendor_id, exc)
        threading.Thread(target=_run, daemon=True).start()
    except Exception as exc:
        app.logger.error("_trigger_post_confirm failed for %s: %s", record_id, exc, exc_info=True)


def _get_vendor_policy_ids(vendor_record_id: str) -> list:
    """Return the list of Insurance Policy record IDs linked to a vendor."""
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    vrec = api.table(base_id, VENDORS_TABLE_ID).get(vendor_record_id)
    return vrec["fields"].get("Insurance Policies") or []


def _get_vendor_policies(vendor_record_id: str) -> list:
    """Return all Insurance Policy records linked to a vendor, sorted by type then expiry."""
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    policy_ids = _get_vendor_policy_ids(vendor_record_id)
    if not policy_ids:
        return []
    tbl = api.table(base_id, INSURANCE_POLICIES_TABLE_ID)
    formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in policy_ids) + ")"
    records = tbl.all(
        formula=formula,
        fields=["Policy Type", "Carrier", "Policy Number", "Effective Date", "Expiration Date", "Status"],
    )
    result = []
    for rec in records:
        pf = rec["fields"]
        result.append({
            "policy_type":     pf.get("Policy Type") or "",
            "carrier":         pf.get("Carrier") or "",
            "policy_number":   pf.get("Policy Number") or "",
            "effective_date":  pf.get("Effective Date") or "",
            "expiration_date": pf.get("Expiration Date") or "",
            "status":          pf.get("Status") or "",
        })
    result.sort(key=lambda p: (p["policy_type"], p["expiration_date"] or ""))
    return result


def _get_active_vendor_ids_for_client(client_id: str) -> list:
    """Return vendor record IDs for all active VCA records linked to the given client.

    Uses the client record's 'Vendor Client Assignments' linked field to avoid the
    ARRAYJOIN formula problem (linked record ARRAYJOIN returns display names, not IDs).
    """
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    try:
        crec = api.table(base_id, CLIENTS_TABLE_ID).get(client_id)
        vca_ids = crec["fields"].get("Vendor Client Assignments") or []
    except Exception:
        return []
    if not vca_ids:
        return []
    vca_tbl = api.table(base_id, VCA_TABLE_ID)
    vca_formula = "AND(OR(" + ", ".join(f"RECORD_ID()='{v}'" for v in vca_ids) + "), {Active})"
    vca_records = vca_tbl.all(formula=vca_formula, fields=["Vendor Link"])
    vendor_ids: list = []
    for vca in vca_records:
        for vid in (vca["fields"].get("Vendor Link") or []):
            if vid not in vendor_ids:
                vendor_ids.append(vid)
    return vendor_ids


def _check_shared_policy_numbers(vendor_record_id: str, extraction_record_id: str) -> None:
    """Check if any policy number for this vendor is shared across 2+ distinct policy types.

    If a conflict is found and not already accepted in Accepted Policy Patterns,
    stores _shared_policy_conflicts in the extraction's Raw JSON and sets it to Pending Review.
    """
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    # Load all policies for this vendor via vendor record link
    pol_tbl = api.table(base_id, INSURANCE_POLICIES_TABLE_ID)
    policy_ids = _get_vendor_policy_ids(vendor_record_id)
    if not policy_ids:
        return
    pol_formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in policy_ids) + ")"
    records = pol_tbl.all(
        formula=pol_formula,
        fields=["Policy Type", "Policy Number"],
    )

    # Group normalized policy numbers → set of policy types
    num_to_types: dict = defaultdict(set)
    num_to_raw: dict = {}
    for rec in records:
        pf = rec["fields"]
        raw_num = (pf.get("Policy Number") or "").strip()
        pol_type = (pf.get("Policy Type") or "").strip()
        if not raw_num or not pol_type:
            continue
        norm = _norm_polnum(raw_num)
        if norm:
            num_to_types[norm].add(pol_type)
            if norm not in num_to_raw:
                num_to_raw[norm] = raw_num

    # Find conflicts: normalized number shared across 2+ distinct types
    conflicts = []
    for norm_num, types in num_to_types.items():
        if len(types) < 2:
            continue
        # Check Accepted Policy Patterns for this vendor + normalized number
        app_tbl = api.table(base_id, ACCEPTED_POLICY_PATTERNS_TABLE_ID)
        accept_formula = (
            f"AND({{Vendor Record ID}} = '{vendor_record_id}', "
            f"{{Policy Number Normalized}} = '{norm_num}')"
        )
        accepted = app_tbl.all(formula=accept_formula, max_records=1)
        if accepted:
            continue
        conflicts.append({
            "norm_number": norm_num,
            "raw_number": num_to_raw.get(norm_num, norm_num),
            "types": sorted(types),
        })

    if not conflicts:
        return

    # Update the extraction record
    ie_tbl = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)
    try:
        rec = ie_tbl.get(extraction_record_id)
    except Exception as exc:
        app.logger.warning("Could not fetch extraction %s: %s", extraction_record_id, exc)
        return

    try:
        raw_json = json.loads(rec["fields"].get("Raw JSON") or "{}")
    except json.JSONDecodeError:
        raw_json = {}

    raw_json["_shared_policy_conflicts"] = conflicts
    ie_tbl.update(extraction_record_id, {
        "Raw JSON": json.dumps(raw_json, indent=2),
        "Review Status": "Pending Review",
        "Review Reason": "Shared policy number",
    }, typecast=True)
    app.logger.info(
        "Queued extraction %s for shared policy number review: %s",
        extraction_record_id,
        [c["norm_number"] for c in conflicts],
    )


# ── Flag computation ─────────────────────────────────────────────────────────

_MC_ENTITY = r"(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Co\.?|Ltd\.?|PLLC|P\.C\.|LLP|LP)"
_MC_SEP    = r"(?:[\r\n]+|[ \t]+and[ \t]+|[ \t]*&[ \t]*|[ \t]+dba[ \t]+)"
_MULTI_COMPANY_RE = re.compile(
    _MC_ENTITY + r"[,.]?\s*" + _MC_SEP + r".*?" + _MC_ENTITY,
    re.IGNORECASE | re.DOTALL,
)


def compute_flags(airtable_fields: dict, raw_data: dict, required_policy_types=None) -> dict:
    """Return a dict of flag_name → bool for one record."""
    flags = {}

    # 1. AI disagreement: AI Disagreements field is non-empty
    ai_raw = (
        airtable_fields.get("AI Disagreements")
        or airtable_fields.get(AI_DISAGREEMENTS_FIELD_ID)
        or ""
    )
    flags["ai_disagreed"] = bool(str(ai_raw).strip())

    # 2. Missing policy fields: any non-empty policy row is missing policy_number
    #    OR is missing both effective_date AND expiration_date
    policies = raw_data.get("policies") or []
    _content_keys = ("policy_number", "carrier", "effective_date", "expiration_date", "coverage_limits")
    if required_policy_types:
        req_indices = _get_required_indices(policies, required_policy_types)
        check_pols = [p for i, p in enumerate(policies) if i in req_indices]
        has_nonrequired = any(True for i in range(len(policies)) if i not in req_indices)
        missing_required_type = (
            any(_find_key_policy(policies, kws)[0] is None for kws, _ in required_policy_types)
            and not has_nonrequired
        )
    else:
        check_pols = policies
        missing_required_type = False
    flags["missing_fields"] = missing_required_type or any(
        isinstance(p, dict)
        and any(str(p.get(k) or "").strip() for k in _content_keys)
        and (
            not (p.get("policy_number") or "").strip()
            or (
                not (p.get("effective_date") or "").strip()
                and not (p.get("expiration_date") or "").strip()
            )
        )
        for p in check_pols
    )

    # 3. Low confidence
    conf = airtable_fields.get("Confidence Score")
    try:
        conf_f = float(conf)
    except (TypeError, ValueError):
        conf_f = None
    flags["low_confidence"] = conf_f is not None and conf_f < 0.85

    # 4. Non-COI document type
    doc_type = (raw_data.get("document_type") or "").strip().upper()
    flags["non_coi"] = bool(doc_type) and doc_type != "COI"

    # 5. Checkbox or policy count differs vs Prior Raw JSON
    flags["changed_vs_prior"] = False
    prior_raw = airtable_fields.get(PRIOR_RAW_JSON_FIELD_NAME) or ""
    if prior_raw:
        try:
            prior_data = json.loads(prior_raw)
            cur_pols = raw_data.get("policies") or []
            pri_pols = prior_data.get("policies") or []
            if len(cur_pols) != len(pri_pols):
                flags["changed_vs_prior"] = True
            else:
                for cp, pp in zip(cur_pols, pri_pols):
                    for chk in ("additional_insured_checked",
                                "waiver_of_subrogation_checked",
                                "primary_noncontributory_checked"):
                        if _norm_tristate(cp.get(chk)) != _norm_tristate(pp.get(chk)):
                            flags["changed_vs_prior"] = True
                            break
                    if flags["changed_vs_prior"]:
                        break
        except (json.JSONDecodeError, AttributeError):
            pass

    # 6. Multi-company named insured
    ni = raw_data.get("named_insured") or ""
    flags["multi_company"] = bool(_MULTI_COMPANY_RE.search(ni))

    # 7. Non-admitted / surplus-lines carrier notice
    _notices = (raw_data.get("notices") or "").lower()
    flags["surplus_lines"] = bool(_notices and any(
        kw in _notices for kw in (
            "surplus", "not licensed", "non-admitted", "nonadmitted",
            "non admitted", "not authorized", "unlicensed", "guaranty fund",
        )
    ))

    # 8. Certificate holder mismatch (RTR only)
    # Fires when the holder is blank, equals the named insured (self-issued),
    # or doesn't match any known RTR client alias.
    if required_policy_types is not None:
        _holder_norm = _norm_holder(raw_data.get("certificate_holder") or "")
        _ni_norm     = _norm_holder(raw_data.get("named_insured") or "")
        if not _holder_norm:
            flags["holder_mismatch"] = True
        elif _holder_norm == _ni_norm:
            flags["holder_mismatch"] = True  # cert issued to itself
        elif not any(
            _norm_holder(a) in _holder_norm or _holder_norm in _norm_holder(a)
            for a in _RTR_CLIENT_ALIASES
        ):
            flags["holder_mismatch"] = True
        else:
            flags["holder_mismatch"] = False
    else:
        flags["holder_mismatch"] = False

    # changed_vs_prior is an informational note, not an action trigger
    flags["needs_look"] = any(
        v for k, v in flags.items()
        if k not in ("needs_look", "changed_vs_prior", "holder_mismatch")
    )

    # needs_decision: routing trigger for "Needs your decision" chip.
    # For RTR certs: fires only on required-field problems (not low_confidence or multi_company).
    # For non-RTR certs: same as needs_look (keeps existing behaviour).
    # Note: possible_duplicate is added by the queue route after cross-record comparison.
    if required_policy_types is not None:
        flags["needs_decision"] = (
            flags["missing_fields"]
            or _required_field_ai_disagreed(ai_raw)
            or flags["non_coi"]
        )
    else:
        flags["needs_decision"] = flags["needs_look"]

    return flags


# ── Second read cell highlighting ─────────────────────────────────────────────

def _highlight_cells(raw_data: dict, second_data: dict) -> set:
    """Return set of (policy_idx, field_name) / (-1, 'named_insured') cells to highlight."""
    highlights = set()
    if not second_data:
        return highlights

    ni_a = (raw_data.get("named_insured") or "").strip().lower()
    ni_b = (second_data.get("named_insured") or "").strip().lower()
    if ni_a != ni_b:
        highlights.add((-1, "named_insured"))

    cur_pols = raw_data.get("policies") or []
    sec_pols = second_data.get("policies") or []

    def _norm_num(v):
        return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())

    check_fields = [
        ("policy_number", _norm_num),
        ("effective_date", lambda v: str(v or "").strip()),
        ("expiration_date", lambda v: str(v or "").strip()),
        ("policy_basis", lambda v: str(v or "").strip().lower()),
        ("additional_insured_checked", _norm_tristate),
        ("waiver_of_subrogation_checked", _norm_tristate),
        ("primary_noncontributory_checked", lambda v: str(bool(v))),
        ("carrier", lambda v: str(v or "").strip().lower()),
    ]
    for i, (cp, sp) in enumerate(zip(cur_pols, sec_pols)):
        for field, norm in check_fields:
            if norm(cp.get(field)) != norm(sp.get(field)):
                highlights.add((i, field))
    # Statement field comparison (certificate-level)
    for stmt_field in ("additional_insured_statement", "waiver_of_subrogation_statement"):
        sa = raw_data.get(stmt_field) or {}
        sb = second_data.get(stmt_field) or {}
        if bool(sa.get("present")) != bool(sb.get("present")):
            highlights.add((-1, stmt_field))
    return highlights


# ── Issue computation (decision panel) ───────────────────────────────────────

_TRISTATE_LABELS = {"checked": "Checked", "not_checked": "Not checked", "not_applicable": "N/A"}


def compute_issues(raw_data: dict, second_data, flags: dict, duplicate_match: str = "", required_policy_types=None, on_file_types: set = None) -> list:
    """Return ordered list of issue dicts for the 'Why this needs you' panel.

    Each issue has:
        id           stable string for JS tracking
        type         ai_disagreement | missing_field | possible_duplicate | non_coi | stmt_box_mismatch
        title        one short sentence shown in the card header
        description  plain-English explanation with the two conflicting values side-by-side
        control      text | tristate | bool | select | none
        field        form input name to drive (may be absent for control=none)
        options      list of {label, value} for tristate/bool/select
        current      current value as a string (pre-selects the control)
    """
    issues = []
    policies = raw_data.get("policies") or []

    # Indices of policies that matter for flags and issue cards
    if required_policy_types:
        req_indices = _get_required_indices(policies, required_policy_types)
    else:
        req_indices = None  # None = include all

    # 1. AI disagreements — one issue per disagreed field
    if second_data:
        sec_policies = second_data.get("policies") or []

        ni_a = (raw_data.get("named_insured") or "").strip()
        ni_b = (second_data.get("named_insured") or "").strip()
        if ni_a.lower() != ni_b.lower():
            issues.append({
                "id": "ai_named_insured",
                "type": "ai_disagreement",
                "title": "Named Insured — AI reads disagree",
                "description": f"Read 1: “{ni_a or '(blank)'}” · Read 2: “{ni_b or '(blank)'}”",
                "control": "text",
                "field": "named_insured",
                "current": ni_a,
                "read1": ni_a,
                "read2": ni_b,
            })

        TRISTATE_PAIRS = [
            ("additional_insured_checked",    "Additional Insured"),
            ("waiver_of_subrogation_checked", "Waiver of Subrogation"),
        ]
        BOOL_PAIRS = [
            ("primary_noncontributory_checked", "Primary & Non-Contributory"),
        ]
        TEXT_PAIRS = [
            ("policy_number",    "Policy Number"),
            ("effective_date",   "Effective Date"),
            ("expiration_date",  "Expiration Date"),
        ]

        for i, pol_a in enumerate(policies):
            if req_indices is not None and i not in req_indices:
                continue
            if i >= len(sec_policies):
                break
            pol_b = sec_policies[i]
            pol_label = (pol_a.get("policy_type") or f"Policy {i + 1}").strip()

            for fld, lbl in TRISTATE_PAIRS:
                va = _norm_tristate(pol_a.get(fld))
                vb = _norm_tristate(pol_b.get(fld))
                if va != vb:
                    issues.append({
                        "id": f"ai_pol{i}_{fld}",
                        "type": "ai_disagreement",
                        "title": f"{pol_label} — {lbl}",
                        "description": f"Read 1: {_TRISTATE_LABELS[va]} · Read 2: {_TRISTATE_LABELS[vb]}",
                        "control": "tristate",
                        "field": f"policy_{i}_{fld}",
                        "current": va,
                        "read1": va,
                        "read2": vb,
                        "options": [
                            {"label": "Checked",     "value": "checked"},
                            {"label": "Not checked", "value": "not_checked"},
                            {"label": "N/A",         "value": "not_applicable"},
                        ],
                    })

            for fld, lbl in BOOL_PAIRS:
                va = bool(pol_a.get(fld))
                vb = bool(pol_b.get(fld))
                if va != vb:
                    issues.append({
                        "id": f"ai_pol{i}_{fld}",
                        "type": "ai_disagreement",
                        "title": f"{pol_label} — {lbl}",
                        "description": (
                            f"Read 1: {'Checked' if va else 'Not checked'} "
                            f"· Read 2: {'Checked' if vb else 'Not checked'}"
                        ),
                        "control": "bool",
                        "field": f"policy_{i}_{fld}",
                        "current": "true" if va else "false",
                        "read1": "true" if va else "false",
                        "read2": "true" if vb else "false",
                        "options": [
                            {"label": "Checked",     "value": "true"},
                            {"label": "Not checked", "value": "false"},
                        ],
                    })

            for fld, lbl in TEXT_PAIRS:
                va = (pol_a.get(fld) or "").strip()
                vb = (pol_b.get(fld) or "").strip()
                if va.upper().replace(" ", "") != vb.upper().replace(" ", ""):
                    issues.append({
                        "id": f"ai_pol{i}_{fld}",
                        "type": "ai_disagreement",
                        "title": f"{pol_label} — {lbl}",
                        "description": f"Read 1: “{va or '(blank)'}” · Read 2: “{vb or '(blank)'}”",
                        "control": "text",
                        "field": f"policy_{i}_{fld}",
                        "current": va,
                        "read1": va,
                        "read2": vb,
                    })

    # 2. Missing field issues
    _content_keys = ("policy_number", "carrier", "effective_date", "expiration_date", "coverage_limits")
    for i, p in enumerate(policies):
        if req_indices is not None and i not in req_indices:
            continue
        pol_label = (p.get("policy_type") or f"Policy {i + 1}").strip()
        has_content = any(str(p.get(k) or "").strip() for k in _content_keys)
        if not has_content:
            continue
        if not (p.get("policy_number") or "").strip():
            issues.append({
                "id": f"missing_pol{i}_number",
                "type": "missing_field",
                "title": f"{pol_label} — policy number is blank",
                "description": "The AI left the policy number empty. Enter it from the certificate.",
                "control": "text",
                "field": f"policy_{i}_policy_number",
                "current": "",
            })
        if not (p.get("effective_date") or "").strip() and not (p.get("expiration_date") or "").strip():
            issues.append({
                "id": f"missing_pol{i}_dates",
                "type": "missing_field",
                "title": f"{pol_label} — both dates are blank",
                "description": "Effective date and expiration date are both empty.",
                "control": "text",
                "field": f"policy_{i}_effective_date",
                "current": "",
            })

    # 2b. Required policy types that are completely absent from the certificate
    if required_policy_types:
        for keywords, label in required_policy_types:
            idx, _ = _find_key_policy(policies, keywords)
            if idx is None:
                # Suppress if this coverage type is on file from another cert for this vendor
                if on_file_types and any(
                    any(k in pt for k in keywords)
                    for pt in on_file_types
                ):
                    continue
                safe_id = label.lower().replace(" ", "_").replace("'", "").replace("/", "")
                issues.append({
                    "id": f"missing_required_{safe_id}",
                    "type": "missing_field",
                    "title": f"{label}: not on file on this certificate",
                    "description": (
                        f"No {label} policy was found. "
                        "If the certificate has one, check that the policy type is labeled correctly."
                    ),
                    "control": "none",
                    "field": None,
                })

    # 3. Non-COI document type
    if flags.get("non_coi"):
        doc_type = (raw_data.get("document_type") or "").strip()
        issues.append({
            "id": "non_coi",
            "type": "non_coi",
            "title": f"Document type reads as “{doc_type}” — is that right?",
            "description": "If this is a Certificate of Insurance, change the type to COI.",
            "control": "select",
            "field": "document_type",
            "current": doc_type,
            "options": [
                {"label": "COI",                  "value": "COI"},
                {"label": "cancellation_notice",  "value": "cancellation_notice"},
                {"label": "endorsement",          "value": "endorsement"},
                {"label": "reinstatement",        "value": "reinstatement"},
                {"label": "unknown",              "value": "unknown"},
            ],
        })

    # 4. Statement vs box mismatch
    for stmt_key, stmt_label, chk_key in (
        ("additional_insured_statement",    "Additional Insured",    "additional_insured_checked"),
        ("waiver_of_subrogation_statement", "Waiver of Subrogation", "waiver_of_subrogation_checked"),
    ):
        stmt = raw_data.get(stmt_key)
        if stmt is None:
            continue
        stmt_present = bool(stmt.get("present"))
        any_checked = any(_norm_tristate(p.get(chk_key)) == "checked" for p in policies)
        if any_checked != stmt_present:
            box_says  = "Checked" if any_checked else "Not checked"
            stmt_says = "Present" if stmt_present else "Absent"
            issues.append({
                "id": f"stmt_{stmt_key}",
                "type": "stmt_box_mismatch",
                "title": f"{stmt_label} — box says {box_says}, statement says {stmt_says}",
                "description": (
                    "The per-policy checkbox and the description of operations statement "
                    "disagree. Confirm which source is correct."
                ),
                "control": "none",
                "field": None,
            })

    # 5. Possible duplicate
    if duplicate_match:
        issues.append({
            "id": "possible_duplicate",
            "type": "possible_duplicate",
            "title": "Possible duplicate certificate",
            "description": (
                f"Policy {duplicate_match} appears in another pending cert "
                "for the same vendor. Use Reject if this is superseded."
            ),
            "control": "none",
            "field": None,
        })

    # 6. Surplus-lines / not-licensed carrier notice
    if flags.get("surplus_lines"):
        _notice_text = (raw_data.get("notices") or "").strip()
        issues.append({
            "id": "surplus_lines_notice",
            "type": "surplus_lines",
            "title": "Non-admitted / surplus-lines carrier",
            "description": (
                (_notice_text + " — " if _notice_text else "")
                + "This certificate contains a notice that a carrier may not be licensed "
                "in this state or is writing on a surplus-lines basis. Verify the carrier "
                "is acceptable before approving."
            ),
            "control": "none",
            "field": None,
        })

    # 7. Expired on arrival — combine into one card per cert (not one per policy)
    _today_for_issues = date.today()
    _expired_pols = []
    for i, p in enumerate(policies):
        if req_indices is not None and i not in req_indices:
            continue
        exp_str = (p.get("expiration_date") or "").strip()
        if not exp_str:
            continue
        try:
            exp_d = date.fromisoformat(exp_str[:10])
        except (ValueError, TypeError):
            continue
        if exp_d < _today_for_issues:
            _expired_pols.append((p, exp_d))

    if _expired_pols:
        # Determine if a newer term is on file for these types (check on_file_types)
        def _type_label_for_pol(p):
            return (p.get("policy_type") or "Policy").strip()

        # Separate into "has newer on file" vs "no newer term"
        _no_newer = []
        _has_newer = []
        for p, exp_d in _expired_pols:
            pt_lower = (p.get("policy_type") or "").lower()
            # on_file_types contains all types on file including this cert's own policies;
            # if the type is on file it suggests there may be coverage, but we can't tell
            # direction without expiration dates. Show quiet note when on file, loud when not.
            if on_file_types and any(pt_lower in t or t in pt_lower for t in on_file_types if len(t) > 3):
                _has_newer.append((p, exp_d))
            else:
                _no_newer.append((p, exp_d))

        def _fmt_expired_title(pol_list):
            """Build a human title like 'GL and WC on this certificate ended 08/27/2025'."""
            labels = [_type_label_for_pol(p) for p, _ in pol_list]
            exps   = [exp_d for _, exp_d in pol_list]
            if len(labels) == 1:
                type_str = labels[0]
            elif len(labels) == 2:
                type_str = f"{labels[0]} and {labels[1]}"
            else:
                type_str = f"{labels[0]}, {labels[1]}, and {len(labels)-2} more"
            if all(e == exps[0] for e in exps):
                return f"{type_str} on this certificate ended {_fmt_date(pol_list[0][0].get('expiration_date') or '') or str(exps[0])}"
            return f"{type_str} on this certificate are expired"

        if _no_newer:
            issues.append({
                "id": "expired_arrival",
                "type": "expired_arrival",
                "title": _fmt_expired_title(_no_newer),
                "description": (
                    "These policies expired before today. Confirm you are reviewing the correct "
                    "certificate, or reject if a renewal is already on file."
                ),
                "control": "none",
                "field": None,
            })
        if _has_newer:
            issues.append({
                "id": "expired_older_term",
                "type": "expired_arrival",
                "title": f"Older term — {', '.join(_type_label_for_pol(p) for p, _ in _has_newer[:2])} on this cert",
                "description": (
                    "A newer term for this coverage type appears to be on file for this vendor. "
                    "This certificate may be an older renewal."
                ),
                "control": "none",
                "field": None,
            })

    return issues


# ── Coverage Snapshot ─────────────────────────────────────────────────────────

CLIENT_REQUIREMENTS_TABLE_ID = "tblFGQ6XgOIHSWtQN"


def compute_snapshot(vendor_id: str, client_id, raw_data: dict, today, extraction_id=None) -> dict:
    """Compute a coverage snapshot for the detail page.

    Returns a dict with client info, per-type status, timeline data, and
    a plain-text 'what changed' summary.

    extraction_id: if provided, policies created from this extraction are excluded
    from the "existing policies" so that what_changed compares against OTHER certs only.
    """
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    # ── 1. Resolve client ───────────────────────────────────────────────────
    resolved_client_id = client_id
    client_name = ""
    client_found = False
    _vrec = None  # cache so we don't fetch vendor record twice

    if not resolved_client_id:
        # Step 1a: vendor's "Client Link" field
        try:
            _vrec = api.table(base_id, VENDORS_TABLE_ID).get(vendor_id)
            client_links = _vrec["fields"].get("Client Link") or []
            if isinstance(client_links, list) and client_links:
                resolved_client_id = client_links[0]
        except Exception:
            pass

    if not resolved_client_id:
        # Step 1b: Vendor Client Assignments (VCA) — two-step lookup
        try:
            if _vrec is None:
                _vrec = api.table(base_id, VENDORS_TABLE_ID).get(vendor_id)
            vca_ids = _vrec["fields"].get("Vendor Client Assignments") or []
            if vca_ids:
                vca_tbl = api.table(base_id, VCA_TABLE_ID)
                vca_formula = "AND(OR(" + ", ".join(f"RECORD_ID()='{v}'" for v in vca_ids) + "), {Active})"
                vca_recs = vca_tbl.all(formula=vca_formula, fields=["Client Link"])
                for vca in vca_recs:
                    cl = vca["fields"].get("Client Link") or []
                    if isinstance(cl, list) and cl:
                        resolved_client_id = cl[0]
                        break
                    elif isinstance(cl, str) and cl:
                        resolved_client_id = cl
                        break
        except Exception:
            pass

    if resolved_client_id:
        try:
            crec = api.table(base_id, CLIENTS_TABLE_ID).get(resolved_client_id)
            client_name = crec["fields"].get("Client Name") or ""
            client_found = True
        except Exception:
            pass

    # ── 2. Client requirements ──────────────────────────────────────────────
    required_labels = []
    if resolved_client_id and client_found:
        try:
            crec = api.table(base_id, CLIENTS_TABLE_ID).get(resolved_client_id)
            req_ids = crec["fields"].get("Client Requirements") or []
            if req_ids:
                req_tbl = api.table(base_id, CLIENT_REQUIREMENTS_TABLE_ID)
                formula = "OR(" + ", ".join(f"RECORD_ID()='{r}'" for r in req_ids) + ")"
                req_records = req_tbl.all(
                    formula=formula,
                    fields=["Policy Type", "Required"],
                )
                for rr in req_records:
                    rf = rr["fields"]
                    if rf.get("Required") and rf.get("Policy Type"):
                        required_labels.append(rf["Policy Type"])
        except Exception:
            pass

    # Fall back to GL + WC if no client requirements found
    if not required_labels:
        required_labels = ["General Liability", "Workers' Comp"]

    # ── 3. Existing policies on file for this vendor ────────────────────────
    # Excludes policies that came from this extraction so what_changed compares
    # against OTHER certificates only (avoids "no change: same terms" false positive).
    existing_policies = []
    try:
        pol_ids = _get_vendor_policy_ids(vendor_id)
        if pol_ids:
            pol_tbl = api.table(base_id, INSURANCE_POLICIES_TABLE_ID)
            formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in pol_ids) + ")"
            pol_records = pol_tbl.all(
                formula=formula,
                fields=["Policy Type", "Policy Number", "Carrier", "Effective Date", "Expiration Date",
                        "Last Updated From Extraction"],
            )
            for pr in pol_records:
                pf = pr["fields"]
                # Skip policies created/updated from this very extraction
                if extraction_id:
                    updated_from = pf.get("Last Updated From Extraction") or []
                    if not isinstance(updated_from, list):
                        updated_from = [updated_from] if updated_from else []
                    if extraction_id in updated_from:
                        continue
                existing_policies.append({
                    "policy_type":     pf.get("Policy Type") or "",
                    "policy_number":   pf.get("Policy Number") or "",
                    "carrier":         pf.get("Carrier") or "",
                    "effective":       pf.get("Effective Date") or "",
                    "expiration":      pf.get("Expiration Date") or "",
                    "is_this_cert":    False,
                })
    except Exception:
        pass

    # ── 4. This cert's policies from raw_data ───────────────────────────────
    cert_policies = []
    for p in (raw_data.get("policies") or []):
        cert_policies.append({
            "policy_type":   p.get("policy_type") or "",
            "policy_number": p.get("policy_number") or "",
            "carrier":       p.get("carrier") or "",
            "effective":     p.get("effective_date") or "",
            "expiration":    p.get("expiration_date") or "",
            "is_this_cert":  True,
        })

    named_insured = raw_data.get("named_insured") or ""

    # ── 5. Per-type status ──────────────────────────────────────────────────
    def _label_matches_keywords(label: str, keywords: tuple) -> bool:
        ll = label.lower()
        return any(k in ll for k in keywords)

    def _type_matches_label(pol_type: str, label: str) -> bool:
        """Check if a policy type string matches a required label."""
        pt = pol_type.lower()
        label_l = label.lower()
        # Direct substring match
        if label_l in pt or pt in label_l:
            return True
        # Keyword-based matching for GL and WC
        if _label_matches_keywords(label, _GL_KEYWORDS) and _label_matches_keywords(pol_type, _GL_KEYWORDS):
            return True
        if _label_matches_keywords(label, _WC_KEYWORDS) and _label_matches_keywords(pol_type, _WC_KEYWORDS):
            return True
        return False

    def _best_policy_for_type(candidates: list, label: str) -> dict | None:
        """Return the best (most recent non-expired, else most recently expired) policy."""
        matches = [p for p in candidates if _type_matches_label(p.get("policy_type") or "", label)]
        if not matches:
            return None
        # Prefer non-expired, most recent expiration
        non_expired = []
        expired = []
        for p in matches:
            exp = _parse_date(p.get("expiration") or "")
            if exp and exp >= today:
                non_expired.append((exp, p))
            else:
                expired.append((exp or date.min, p))
        if non_expired:
            non_expired.sort(key=lambda x: x[0], reverse=True)
            return non_expired[0][1]
        if expired:
            expired.sort(key=lambda x: x[0], reverse=True)
            return expired[0][1]
        return None

    types_data = []
    for label in required_labels:
        cert_match = _best_policy_for_type(cert_policies, label)
        existing_match = _best_policy_for_type(existing_policies, label)

        best = cert_match or existing_match
        if cert_match:
            source = "this_cert"
        elif existing_match:
            source = "already_on_file"
        else:
            source = "not_on_file"

        if best:
            exp_d = _parse_date(best.get("expiration") or "")
            eff_d = _parse_date(best.get("effective") or "")
            expired_flag = bool(exp_d and exp_d < today)
            days_left = None
            if exp_d and not expired_flag:
                days_left = (exp_d - today).days

            types_data.append({
                "label":         label,
                "status":        "on_file",
                "source":        source,
                "policy_number": best.get("policy_number") or "",
                "carrier":       best.get("carrier") or "",
                "effective":     best.get("effective") or "",
                "expiration":    best.get("expiration") or "",
                "days_left":     days_left,
                "expired":       expired_flag,
            })
        else:
            types_data.append({
                "label":         label,
                "status":        "not_on_file",
                "source":        "not_on_file",
                "policy_number": "",
                "carrier":       "",
                "effective":     "",
                "expiration":    "",
                "days_left":     None,
                "expired":       False,
            })

    # ── 6. Timeline data ────────────────────────────────────────────────────
    timeline = {}
    all_policies = existing_policies + cert_policies
    for label in required_labels:
        terms = [p for p in all_policies if _type_matches_label(p.get("policy_type") or "", label)]
        # Sort by effective date
        def _sort_key(p):
            d = _parse_date(p.get("effective") or "")
            return d or date.min
        terms.sort(key=_sort_key)
        timeline[label] = [
            {
                "effective":     t.get("effective") or "",
                "expiration":    t.get("expiration") or "",
                "carrier":       t.get("carrier") or "",
                "policy_number": t.get("policy_number") or "",
                "is_this_cert":  t.get("is_this_cert", False),
            }
            for t in terms
        ]

    # ── 7. What changed ─────────────────────────────────────────────────────
    what_changed = []
    for label in required_labels:
        cert_match = _best_policy_for_type(cert_policies, label)
        existing_match = _best_policy_for_type(existing_policies, label)

        if not cert_match:
            continue

        cert_exp = _parse_date(cert_match.get("expiration") or "")
        cert_eff = _parse_date(cert_match.get("effective") or "")

        if not existing_match:
            what_changed.append(f"New {label} coverage for this vendor.")
        else:
            existing_exp = _parse_date(existing_match.get("expiration") or "")
            existing_eff = _parse_date(existing_match.get("effective") or "")

            if cert_exp and existing_exp and cert_exp > existing_exp:
                prev_str = _fmt_date(existing_match.get("expiration") or "") or str(existing_exp)
                what_changed.append(f"Renews {label}. Previous term ended {prev_str}.")
            elif cert_eff and existing_exp and cert_eff > existing_exp:
                # Gap between last expiration and new effective
                gap_start = _fmt_date(existing_match.get("expiration") or "") or str(existing_exp)
                gap_end = _fmt_date(cert_match.get("effective") or "") or str(cert_eff)
                what_changed.append(f"Closes a {label} gap ({gap_start} to {gap_end}).")
            else:
                # Cert pol number differs from existing
                cert_norm = _norm_polnum(cert_match.get("policy_number") or "")
                exist_norm = _norm_polnum(existing_match.get("policy_number") or "")
                if cert_norm and exist_norm and cert_norm != exist_norm:
                    what_changed.append(f"New {label} policy number (different from file).")
                else:
                    what_changed.append(f"No change: same {label} terms already on file.")

    return {
        "client_id":     resolved_client_id,
        "client_name":   client_name,
        "client_found":  client_found,
        "named_insured": named_insured,
        "types":         types_data,
        "timeline":      timeline,
        "what_changed":  what_changed,
    }


def _pace_stats(batch_rows: list) -> dict:
    """Compute queue pace metrics shown in the queue header."""
    now = datetime.now(timezone.utc)
    week_start = (now - timedelta(days=7)).isoformat()
    month_start = (now - timedelta(days=30)).isoformat()
    PENDING = {"Pending Review"}
    APPROVED = {"Approved", "Approved + Edited", "Auto-Approved"}

    pending_dated = [r for r in batch_rows if r.get("review_status") in PENDING and r.get("received_at")]
    oldest_waiting = min((r["received_at"] for r in pending_dated), default=None)

    received_this_week = sum(1 for r in batch_rows if (r.get("received_at") or "") >= week_start)
    confirmed_this_week = sum(
        1 for r in batch_rows
        if r.get("review_status") in APPROVED and (r.get("reviewed_at") or "") >= week_start
    )

    durations = []
    for r in batch_rows:
        recv = r.get("received_at") or ""
        revw = r.get("reviewed_at") or ""
        if recv >= month_start and recv and revw:
            try:
                d1 = datetime.fromisoformat(recv.replace("Z", "+00:00"))
                d2 = datetime.fromisoformat(revw.replace("Z", "+00:00"))
                days = (d2 - d1).total_seconds() / 86400
                if 0 <= days < 365:
                    durations.append(days)
            except Exception:
                pass
    avg_days = round(sum(durations) / len(durations), 1) if durations else None

    return {
        "oldest_waiting": oldest_waiting,
        "received_this_week": received_this_week,
        "confirmed_this_week": confirmed_this_week,
        "avg_days_to_confirm": avg_days,
    }


# ── Queue ─────────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return redirect(url_for("pulse"))


@app.route("/follow-ups")
@login_required
def follow_ups():
    """Unified follow-ups page — vendor × coverage pipeline view."""
    client_id = request.args.get("client_id", RTR_CLIENT_ID)
    stage_filter = request.args.get("stage", "all")
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    today = date.today()

    # Load clients
    clients_raw = api.table(base_id, CLIENTS_TABLE_ID).all(fields=["Client Name"])
    clients = sorted(
        [{"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]} for r in clients_raw],
        key=lambda c: c["name"],
    )
    client_name = next((c["name"] for c in clients if c["id"] == client_id), client_id)

    # Try to find active audit for window defaulting and payment amounts
    audit_info = None
    vendor_amounts: dict = {}
    try:
        from audit_store import _conn as _audit_conn
        with _audit_conn() as aconn:
            arow = aconn.execute(
                "SELECT id, start_date, end_date, client_name FROM audits "
                "WHERE status NOT IN ('submitted', 'archived') ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if arow:
                audit_info = dict(arow)
                pay_rows = aconn.execute(
                    """SELECT pc.vendor_id, SUM(p.amount) as total_paid
                       FROM payments p
                       JOIN payee_classifications pc ON pc.payee_raw = p.payee_raw AND pc.audit_id = p.audit_id
                       WHERE p.audit_id = ? AND p.excluded = 0 AND pc.payee_group = 'ccs_vendor' AND pc.vendor_id != ''
                       GROUP BY pc.vendor_id""",
                    (audit_info["id"],)
                ).fetchall()
                for pr in pay_rows:
                    vendor_amounts[pr["vendor_id"]] = float(pr["total_paid"])
    except Exception as exc:
        app.logger.warning("follow-ups audit load: %s", exc)

    # Window: use audit period as default if no explicit window
    window_start_str = request.args.get("window_start", "").strip()
    window_end_str = request.args.get("window_end", "").strip()
    auto_window = False
    if not window_start_str and not window_end_str and audit_info:
        window_start_str = audit_info["start_date"]
        window_end_str = audit_info["end_date"]
        auto_window = True
    window_start = _parse_date(window_start_str)
    window_end = _parse_date(window_end_str)
    has_window = bool(window_start and window_end and window_start <= window_end)

    # Load all gap requests for client
    gap_tbl = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    try:
        gap_records = gap_tbl.all(fields=[
            "Vendor", "Client", "Coverage Type", "Gap Start", "Gap End",
            "Status", "Cadence Status", "Sent At", "Requested On",
        ])
    except Exception:
        gap_records = []

    # Build best-request lookup: "vendor_id:coverage_type" -> request dict
    request_by_key: dict = {}
    for gr in gap_records:
        gf = gr["fields"]
        if client_id not in (gf.get("Client") or []):
            continue
        cadence = gf.get("Cadence Status") or gf.get("Status") or "Draft"
        for vid in (gf.get("Vendor") or []):
            cov = (gf.get("Coverage Type") or "").strip()
            key = f"{vid}:{cov}"
            if key not in request_by_key or _stage_rank(cadence) > _stage_rank(request_by_key[key]["cadence_status"]):
                request_by_key[key] = {
                    "id": gr["id"],
                    "cadence_status": cadence,
                    "gap_start": gf.get("Gap Start") or "",
                    "gap_end": gf.get("Gap End") or "",
                    "sent_at": gf.get("Sent At") or "",
                    "requested_on": gf.get("Requested On") or "",
                }

    vendor_ids = _get_active_vendor_ids_for_client(client_id)
    vendors_tbl = api.table(base_id, VENDORS_TABLE_ID)
    pol_tbl = api.table(base_id, INSURANCE_POLICIES_TABLE_ID)

    vendors_out = []
    for vendor_id in vendor_ids:
        try:
            vrec = vendors_tbl.get(vendor_id)
            vf = vrec["fields"]
        except Exception:
            vrec = {"fields": {}}
            vf = {}
        vendor_name = vf.get("Vendor Name") or vendor_id
        contact_email = vf.get("Vendor Email") or ""
        agent_email = vf.get("Agent Email") or ""
        agent_name = vf.get("Agent Name") or ""
        agent_agency = vf.get("Agent Agency") or ""
        agent_phone = vf.get("Agent Phone") or ""
        agent_bounced = bool(vf.get("Agent Bounced"))
        primary_ct = vf.get("Primary Contact") or "Agent"
        has_usable_contact = bool((agent_email and not agent_bounced) or contact_email)

        policy_ids = vrec["fields"].get("Insurance Policies") or []
        if policy_ids:
            pol_formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in policy_ids) + ")"
            pol_records = pol_tbl.all(formula=pol_formula, fields=["Policy Type", "Policy Number", "Effective Date", "Expiration Date", "Carrier"])
        else:
            pol_records = []

        policies_by_type: dict = defaultdict(list)
        for prec in pol_records:
            pf = prec["fields"]
            pt = (pf.get("Policy Type") or "").lower()
            entry = {
                "policy_number": pf.get("Policy Number") or "",
                "effective_date": pf.get("Effective Date") or "",
                "expiration_date": pf.get("Expiration Date") or "",
                "carrier": pf.get("Carrier") or "",
            }
            if _pol_matches_audit_type(pt, _GL_KEYWORDS):
                policies_by_type["General Liability"].append(entry)
            elif any(k in pt for k in _WC_KEYWORDS):
                policies_by_type["Workers' Comp"].append(entry)

        coverage_items = []
        for kws, label in RTR_REQUIRED_POLICY_TYPES:
            pols = policies_by_type.get(label) or []
            req_key = f"{vendor_id}:{label}"
            existing_req = request_by_key.get(req_key)

            gap_kind = None
            gap_start_val = ""
            gap_end_val = ""
            gap_days = None
            window_status = None

            if has_window:
                w_terms = []
                w_has_any = False
                for prec in pol_records:
                    pf = prec["fields"]
                    pt = (pf.get("Policy Type") or "").lower()
                    if not _pol_matches_audit_type(pt, kws):
                        continue
                    w_has_any = True
                    eff = _parse_date(pf.get("Effective Date"))
                    exp = _parse_date(pf.get("Expiration Date"))
                    if eff and exp and eff <= window_end and exp >= window_start:
                        w_terms.append({"effective": eff.isoformat(), "expiration": exp.isoformat()})
                w_terms.sort(key=lambda t: t["effective"])
                wstatus, wdays, _ = _compute_vendor_audit_status(w_terms, window_start, window_end, w_has_any)
                window_status = {"status": wstatus, "days_not_on_file": wdays}
                if wstatus != "full":
                    gap_kind = "missing" if not w_has_any else "date_gap"
                    gap_days = wdays
            else:
                if not pols:
                    gap_kind = "missing"
                else:
                    dg = _compute_date_gaps(pols, today)
                    if dg:
                        g = dg[0]
                        gap_kind = "expired" if g.get("expired") else "date_gap"
                        gap_start_val = g["gap_start"] or ""
                        gap_end_val = g.get("gap_end") or ""
                        gap_days = g["days"]

            if existing_req:
                cadence = existing_req["cadence_status"]
                stage = _cadence_to_stage(cadence)
                req_gap_start = existing_req.get("gap_start") or gap_start_val
                req_gap_end = existing_req.get("gap_end") or gap_end_val
                sent_at = existing_req.get("sent_at") or ""
                days_until = None
                next_due = ""
                if sent_at:
                    cadence_map = {
                        "Sent — Awaiting Reply": 4, "Day 4 Due": 4, "Day 4 Drafted": 8,
                        "Day 8 Due": 8, "Day 8 Drafted": 14, "Day 14 — Call": 14,
                        "Day 21 — Client Update": 21,
                    }
                    d0 = _parse_date(sent_at)
                    offset = cadence_map.get(cadence)
                    if d0 and offset is not None:
                        nd = d0 + timedelta(days=offset)
                        next_due = nd.isoformat()
                        days_until = (nd - today).days
            elif gap_kind is not None:
                stage = "missing"
                req_gap_start = gap_start_val
                req_gap_end = gap_end_val
                sent_at = ""
                days_until = None
                next_due = ""
            else:
                continue

            coverage_items.append({
                "coverage_type": label,
                "stage": stage,
                "gap_kind": gap_kind,
                "gap_start": req_gap_start if existing_req else gap_start_val,
                "gap_end": req_gap_end if existing_req else gap_end_val,
                "gap_days": gap_days,
                "window_status": window_status,
                "request_id": existing_req["id"] if existing_req else None,
                "request_cadence": existing_req["cadence_status"] if existing_req else "",
                "request_sent_at": sent_at,
                "request_next_due": next_due,
                "request_days_until": days_until,
            })

        if not coverage_items:
            continue

        stage_order_pri = ["waiting", "missing", "drafted", "sent", "received"]
        stages_present = {ci["stage"] for ci in coverage_items}
        primary_stage = min(stages_present, key=lambda s: stage_order_pri.index(s) if s in stage_order_pri else 99)

        vendors_out.append({
            "vendor_id": vendor_id,
            "vendor_name": vendor_name,
            "contact_email": contact_email,
            "agent_email": agent_email,
            "agent_name": agent_name,
            "agent_agency": agent_agency,
            "agent_phone": agent_phone,
            "agent_bounced": agent_bounced,
            "primary_contact": primary_ct,
            "has_usable_contact": has_usable_contact,
            "amount_paid": vendor_amounts.get(vendor_id),
            "coverage_items": coverage_items,
            "primary_stage": primary_stage,
            "all_stages": " ".join(sorted(stages_present)),
        })

    if vendor_amounts:
        vendors_out.sort(key=lambda v: (-(v["amount_paid"] or 0), v["vendor_name"].lower()))
    else:
        vendors_out.sort(key=lambda v: v["vendor_name"].lower())

    stage_counts: dict = {s: 0 for s in ["missing", "drafted", "sent", "waiting", "received"]}
    for v in vendors_out:
        for ci in v["coverage_items"]:
            stage_counts[ci["stage"]] = stage_counts.get(ci["stage"], 0) + 1
    total_items = sum(stage_counts.values())

    return render_template(
        "follow-ups.html",
        vendors=vendors_out,
        client_id=client_id,
        client_name=client_name,
        clients=clients,
        today=today.isoformat(),
        has_window=has_window,
        window_start=window_start_str,
        window_end=window_end_str,
        auto_window=auto_window,
        audit_info=audit_info,
        has_payments=bool(vendor_amounts),
        stage_counts=stage_counts,
        total_items=total_items,
        stage_filter=stage_filter,
    )


@app.route("/queue")
@login_required
def queue():
    chip = request.args.get("filter", "pending")
    table = _ie_table()
    all_records = table.all()
    received_map = _received_at_map()

    rows = []
    for rec in all_records:
        f = rec["fields"]
        try:
            raw_data = json.loads(f.get("Raw JSON") or "{}")
        except json.JSONDecodeError:
            raw_data = {}

        review_status = (f.get("Review Status") or "").strip()
        source_filename = f.get("Source Filename") or ""
        _rpt = RTR_REQUIRED_POLICY_TYPES if _is_billy_rtr(source_filename) else None
        flags = compute_flags(f, raw_data, _rpt)

        # Received At: prefer the field written by airtable_importer, fall back to doc map
        received_at = (
            f.get("Received At")
            or received_map.get(source_filename)
            or ""
        )

        rows.append({
            "id": rec["id"],
            "source_filename": source_filename,
            "named_insured": f.get("Named Insured") or "",
            "review_status": review_status,
            "review_reason": f.get("Review Reason") or "",
            "confidence": f.get("Confidence Score"),
            "policies_count": f.get("Policies Count") or 0,
            "extraction_at": f.get("Extraction Processed At") or "",
            "received_at": received_at,
            "reviewed_at": f.get("Reviewed At") or "",
            "flags": flags,
            "_policies": raw_data.get("policies") or [],
        })

    PENDING_SET = {REVIEW_STATUS_PENDING_REVIEW}
    APPROVED_SET = {REVIEW_STATUS_APPROVED, REVIEW_STATUS_APPROVED_EDITED, REVIEW_STATUS_AUTO_APPROVED}
    REJECTED_SET = {REVIEW_STATUS_REJECTED}

    # possible_duplicate: same normalized NI + at least one identical policy number & effective date
    for r in rows:
        r["flags"]["possible_duplicate"] = False
        r["duplicate_match"] = ""

    ni_groups: dict = defaultdict(list)
    for r in rows:
        if r["review_status"] in PENDING_SET and r["named_insured"]:
            ni_key = _norm_ni(r["named_insured"])
            if ni_key:
                ni_groups[ni_key].append(r)

    for group in ni_groups.values():
        if len(group) < 2:
            continue
        # {(norm_polnum, effective_date): original_policy_number} per row
        pol_maps = []
        for r in group:
            m = {}
            for p in r["_policies"]:
                norm = _norm_polnum(p.get("policy_number") or "")
                eff = (p.get("effective_date") or "").strip()
                if norm and eff:
                    m[(norm, eff)] = (p.get("policy_number") or "").strip()
            pol_maps.append(m)

        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                shared = set(pol_maps[i]) & set(pol_maps[j])
                if not shared:
                    continue
                norm_key = next(iter(shared))
                orig_num = pol_maps[i][norm_key]
                eff_date = norm_key[1]
                match_text = f"{orig_num} / {eff_date}" if eff_date else orig_num
                for r_idx in (i, j):
                    r = group[r_idx]
                    r["flags"]["possible_duplicate"] = True
                    r["flags"]["needs_look"] = True
                    r["flags"]["needs_decision"] = True
                    if not r["duplicate_match"]:
                        r["duplicate_match"] = match_text

    # Batch filter
    batch = request.args.get("batch", "rtr")
    if batch == "rtr":
        batch_rows = [r for r in rows if _is_billy_rtr(r["source_filename"])]
    else:
        batch_rows = [r for r in rows if not _is_billy_rtr(r["source_filename"])]

    if chip == "ai_disagreed":
        filtered = [r for r in batch_rows if r["flags"]["ai_disagreed"] and r["review_status"] in PENDING_SET]
    elif chip == "needs_look":
        filtered = [r for r in batch_rows if r["flags"]["needs_look"] and not r["flags"]["ai_disagreed"] and r["review_status"] in PENDING_SET]
    elif chip == "approved":
        filtered = [r for r in batch_rows if r["review_status"] in APPROVED_SET]
    elif chip == "rejected":
        filtered = [r for r in batch_rows if r["review_status"] in REJECTED_SET]
    else:  # "pending" or default — show all pending
        filtered = [r for r in batch_rows if r["review_status"] in PENDING_SET]

    # Step 1: sort oldest-first by received_at (fall back to extraction_at)
    filtered.sort(key=lambda r: r["received_at"] or r["extraction_at"])
    # Step 2: stable-sort by priority group (preserves oldest-first within each group)
    def _queue_priority(r):
        if r["flags"]["ai_disagreed"]:
            return 0
        if r["flags"]["possible_duplicate"]:
            return 1
        return 2
    filtered.sort(key=_queue_priority)

    counts = {
        "pending": sum(1 for r in batch_rows if r["review_status"] in PENDING_SET),
        "ai_disagreed": sum(1 for r in batch_rows if r["flags"]["ai_disagreed"] and r["review_status"] in PENDING_SET),
        # needs_look excludes ai_disagreed so tile sub-counts are non-overlapping
        "needs_look": sum(1 for r in batch_rows if r["flags"]["needs_look"] and not r["flags"]["ai_disagreed"] and r["review_status"] in PENDING_SET),
        "approved": sum(1 for r in batch_rows if r["review_status"] in APPROVED_SET),
        "rejected": sum(1 for r in batch_rows if r["review_status"] in REJECTED_SET),
    }

    # Store queue order and duplicate matches in session for detail page
    session["queue_ids"] = [r["id"] for r in filtered]
    session["queue_batch"] = batch
    session["queue_chip"] = chip
    session["queue_duplicate_matches"] = {
        r["id"]: r["duplicate_match"]
        for r in rows
        if r.get("duplicate_match")
    }

    # Clean batch: high-confidence, no flags, pending — eligible for batch confirm
    batch_clean = [
        r for r in batch_rows
        if r["review_status"] in PENDING_SET
        and r.get("confidence") is not None and r["confidence"] >= 0.95
        and not any(r["flags"].get(k) for k in (
            "ai_disagreed", "missing_fields", "surplus_lines", "non_coi",
            "holder_mismatch", "possible_duplicate",
        ))
    ] if chip == "pending" else []

    weekly_learnings = _get_weekly_learnings()
    pace = _pace_stats(batch_rows)
    return render_template("queue.html", rows=filtered, chip=chip, counts=counts, batch=batch,
                           weekly_learnings=weekly_learnings, batch_clean=batch_clean,
                           pace=pace)


# ── Batch confirm ────────────────────────────────────────────────────────────

@app.route("/batch-confirm", methods=["POST"])
@login_required
@csrf_protect
def batch_confirm():
    record_ids = request.form.getlist("record_ids")
    if not record_ids:
        flash("No certificates selected.", "warning")
        return redirect(url_for("queue"))
    table = _ie_table()
    now_iso = datetime.now(timezone.utc).isoformat()
    confirmed = 0
    for rid in record_ids[:50]:  # safety cap
        try:
            table.update(rid, {
                "Review Status": REVIEW_STATUS_AUTO_APPROVED,
                "Reviewed At": now_iso,
            }, typecast=True)
            confirmed += 1
        except Exception:
            app.logger.warning("batch_confirm: failed to update %s", rid)
    flash(f"{confirmed} certificate(s) confirmed.", "success")
    return redirect(url_for("queue"))


# ── Detail ────────────────────────────────────────────────────────────────────

@app.route("/detail/<record_id>")
@login_required
def detail(record_id):
    table = _ie_table()
    rec = table.get(record_id)
    if not rec:
        abort(404)

    f = rec["fields"]
    try:
        raw_data = json.loads(f.get("Raw JSON") or "{}")
    except json.JSONDecodeError:
        raw_data = {}

    second_json_str = f.get(SECOND_READ_JSON_FIELD_ID) or ""
    second_data = None
    if second_json_str:
        try:
            second_data = json.loads(second_json_str)
        except json.JSONDecodeError:
            pass

    highlight_cells = _highlight_cells(raw_data, second_data)

    prior_raw = f.get(PRIOR_RAW_JSON_FIELD_NAME) or ""
    prior_data = None
    if prior_raw:
        try:
            prior_data = json.loads(prior_raw)
        except json.JSONDecodeError:
            pass

    source_page_url = None
    source_page_full_url = None
    attachments = f.get(SOURCE_PAGE_FIELD)
    if isinstance(attachments, list) and attachments:
        # Prefer the last image/* attachment (high-res PNG added by rerender script);
        # fall back to thumbnail of the first PDF attachment.
        image_atts = [a for a in attachments if (a.get("type") or "").startswith("image/")]
        pdf_atts   = [a for a in attachments if not (a.get("type") or "").startswith("image/")]
        if image_atts:
            att = image_atts[-1]  # last = most recently added high-res PNG
            source_page_url = att.get("url")
            source_page_full_url = att.get("url")
        elif pdf_atts:
            att = pdf_atts[0]
            thumbs = att.get("thumbnails") or {}
            large = (thumbs.get("large") or {}).get("url")
            source_page_url = large or att.get("url")

    ai_raw = (
        f.get("AI Disagreements") or f.get(AI_DISAGREEMENTS_FIELD_ID) or ""
    )
    ai_disagrees = [d.strip() for d in str(ai_raw).split("\n") if d.strip()] if ai_raw else []

    source_filename = f.get("Source Filename") or ""
    required_policy_types = RTR_REQUIRED_POLICY_TYPES if _is_billy_rtr(source_filename) else None
    flags = compute_flags(f, raw_data, required_policy_types)

    # Received At — prefer the stored field, fall back to Incoming Documents lookup
    received_at = f.get("Received At") or ""
    if not received_at:
        received_map = _received_at_map()
        received_at = received_map.get(source_filename, "")

    # Look up all Insurance Policies on file for this vendor across all their certificates
    vendor_policies = []
    _mv = f.get("Matched Vendor")
    _vendor_id = None
    if isinstance(_mv, list) and _mv:
        _vendor_id = _mv[0] if isinstance(_mv[0], str) else None
    elif isinstance(_mv, str) and _mv.strip():
        _vendor_id = _mv.strip()
    if _vendor_id:
        try:
            vendor_policies = _get_vendor_policies(_vendor_id)
        except Exception:
            vendor_policies = []
    on_file_types = {p["policy_type"].lower() for p in vendor_policies if p.get("policy_type")}

    # Coverage snapshot (Part 2)
    _mv_clients = f.get("Matched Client") or []
    snapshot = None
    if _vendor_id:
        try:
            snapshot = compute_snapshot(
                vendor_id=_vendor_id,
                client_id=(_mv_clients[0] if isinstance(_mv_clients, list) and _mv_clients else None),
                raw_data=raw_data,
                today=date.today(),
                extraction_id=record_id,
            )
        except Exception:
            app.logger.exception("compute_snapshot failed for %s", _vendor_id)

    duplicate_match = (session.get("queue_duplicate_matches") or {}).get(record_id, "")
    issues = compute_issues(raw_data, second_data, flags, duplicate_match, required_policy_types, on_file_types)

    _policies = raw_data.get("policies") or []
    gl_idx, gl_policy = _find_key_policy(_policies, _GL_KEYWORDS)
    wc_idx, wc_policy = _find_key_policy(_policies, _WC_KEYWORDS)
    if required_policy_types:
        _req_indices = _get_required_indices(_policies, required_policy_types)
    else:
        _req_indices = {i for i in (gl_idx, wc_idx) if i is not None}
    other_policy_count = sum(1 for i in range(len(_policies)) if i not in _req_indices)
    primary_issues, secondary_issues = _split_issues(issues, _req_indices)
    contact_email = ", ".join(raw_data.get("contact_emails") or []) or ""

    corrections_logged = request.args.get("corrections_logged", type=int, default=None)

    _base_id = clean_base_id(config.AIRTABLE_BASE_ID) if config.AIRTABLE_BASE_ID else ""
    airtable_url = (
        f"https://airtable.com/{_base_id}/{EXTRACTIONS_TABLE_ID}/{record_id}"
        if _base_id else ""
    )

    # Next/previous navigation from session queue
    queue_ids = session.get("queue_ids") or []
    queue_batch = session.get("queue_batch", "rtr")
    queue_chip = session.get("queue_chip", "pending")
    prev_id = None
    next_id = None
    queue_position = None
    queue_total = len(queue_ids)
    if record_id in queue_ids:
        idx = queue_ids.index(record_id)
        queue_position = idx + 1
        prev_id = queue_ids[idx - 1] if idx > 0 else None
        next_id = queue_ids[idx + 1] if idx < len(queue_ids) - 1 else None

    return render_template(
        "detail.html",
        record_id=record_id,
        f=f,
        raw_data=raw_data,
        second_data=second_data,
        highlight_cells=highlight_cells,
        prior_data=prior_data,
        source_page_url=source_page_url,
        source_page_full_url=source_page_full_url,
        ai_disagrees=ai_disagrees,
        flags=flags,
        issues=issues,
        primary_issues=primary_issues,
        secondary_issues=secondary_issues,
        gl_idx=gl_idx,
        gl_policy=gl_policy,
        wc_idx=wc_idx,
        wc_policy=wc_policy,
        other_policy_count=other_policy_count,
        contact_email=contact_email,
        corrections_logged=corrections_logged,
        airtable_url=airtable_url,
        prev_id=prev_id,
        next_id=next_id,
        queue_batch=queue_batch,
        queue_chip=queue_chip,
        queue_position=queue_position,
        queue_total=queue_total,
        approve_button_label="Required fields are correct" if required_policy_types else "Data is correct",
        required_policy_types=required_policy_types,
        vendor_policies=vendor_policies,
        vendor_id=_vendor_id or "",
        snapshot=snapshot,
        received_at=received_at,
    )


# ── Action ────────────────────────────────────────────────────────────────────

@app.route("/action/<record_id>", methods=["POST"])
@login_required
@csrf_protect
def action(record_id):
    table = _ie_table()
    rec = table.get(record_id)
    if not rec:
        abort(404)

    f = rec["fields"]
    source_filename = f.get("Source Filename") or ""
    try:
        original_data = json.loads(f.get("Raw JSON") or "{}")
    except json.JSONDecodeError:
        original_data = {}

    action_type = request.form.get("action", "").strip()
    why_note = (request.form.get("why") or "").strip()
    now_iso = datetime.now(timezone.utc).isoformat()

    # Determine next cert in queue (if any) before modifying session
    queue_ids = session.get("queue_ids") or []
    queue_batch = session.get("queue_batch", "rtr")
    queue_chip = session.get("queue_chip", "pending")
    next_id = None
    if record_id in queue_ids:
        idx = queue_ids.index(record_id)
        next_id = queue_ids[idx + 1] if idx < len(queue_ids) - 1 else None

    if action_type == "approve":
        required_policy_types = RTR_REQUIRED_POLICY_TYPES if _is_billy_rtr(source_filename) else None
        corrections_count = _handle_approve(
            table, record_id, source_filename, original_data, why_note, now_iso, required_policy_types,
        )
        if next_id:
            if corrections_count > 0:
                flash(f"Data confirmed — {corrections_count} correction(s) saved.", "success")
            else:
                flash("Data confirmed — no corrections.", "success")
            return redirect(url_for("detail", record_id=next_id))
        return redirect(url_for("detail", record_id=record_id,
                                corrections_logged=corrections_count))

    if action_type == "reject":
        table.update(record_id, {"Review Status": REVIEW_STATUS_REJECTED}, typecast=True)
        if why_note:
            _write_correction(source_filename, record_id, "Review Action",
                              "Pending Review", "Rejected", why_note, now_iso)
        if next_id:
            return redirect(url_for("detail", record_id=next_id))
        return redirect(url_for("queue", batch=queue_batch, filter=queue_chip))

    if action_type == "escalate":
        table.update(record_id, {"Review Status": REVIEW_STATUS_ESCALATED}, typecast=True)
        if why_note:
            _write_correction(source_filename, record_id, "Review Action",
                              "Pending Review", "Escalated to GC", why_note, now_iso)
        if next_id:
            return redirect(url_for("detail", record_id=next_id))
        return redirect(url_for("queue", batch=queue_batch, filter=queue_chip))

    if action_type == "save_draft":
        corrections_count = _handle_save_draft(
            table, record_id, source_filename, original_data, why_note, now_iso,
        )
        if corrections_count:
            flash(f"{corrections_count} correction(s) saved — still in queue.", "info")
        else:
            flash("No changes detected.", "info")
        return redirect(url_for("detail", record_id=record_id))

    abort(400)


_TOP_LEVEL_TEXT_FIELDS = ("named_insured", "certificate_holder",
                          "certificate_date", "description_of_operations")

_STATEMENT_FIELDS = (
    ("additional_insured_statement", "ai_stmt"),
    ("waiver_of_subrogation_statement", "wos_stmt"),
)


def _collect_corrections(original_data):
    """Compare submitted form values with original_data; return (corrections, new_policies).

    corrections is a list of (field_path, old_str, new_str) tuples.
    new_policies is the updated policies list reflecting form submissions.

    A field is only compared if it's present in the form submission — this keeps
    older form submissions (that don't include newer fields like document_type)
    from spuriously logging corrections.
    """
    corrections = []

    # Top-level text fields (named_insured, certificate_holder, certificate_date,
    # description_of_operations, document_type)
    for field in _TOP_LEVEL_TEXT_FIELDS + ("document_type",):
        if field not in request.form:
            continue
        submitted = (request.form.get(field) or "").strip() or None
        original = original_data.get(field)
        if isinstance(original, str):
            original = original.strip() or None
        elif original is None:
            original = None
        else:
            original = str(original)
        if submitted != original:
            corrections.append((field, str(original or ""), str(submitted or "")))

    # Statement fields (additional_insured_statement, waiver_of_subrogation_statement)
    for stmt_key, form_prefix in _STATEMENT_FIELDS:
        orig_stmt = original_data.get(stmt_key) or {}
        # Only compare if present in form (defensive — old form submissions won't have these)
        if f"{form_prefix}_present" in request.form or f"{form_prefix}_quote" in request.form \
                or f"{form_prefix}_policies_named" in request.form:
            new_present = request.form.get(f"{form_prefix}_present") == "true"
            new_quote = (request.form.get(f"{form_prefix}_quote") or "").strip() or None
            raw_named = (request.form.get(f"{form_prefix}_policies_named") or "").strip()
            new_named = [p.strip() for p in raw_named.split(",") if p.strip()] or None

            orig_present = bool(orig_stmt.get("present"))
            if new_present != orig_present:
                corrections.append((f"{stmt_key}.present", str(orig_present), str(new_present)))
            orig_quote = orig_stmt.get("quote") or ""
            if (new_quote or "") != orig_quote:
                corrections.append((f"{stmt_key}.quote", orig_quote, str(new_quote or "")))
            orig_named_list = orig_stmt.get("policies_named") or []
            if not isinstance(orig_named_list, list):
                orig_named_list = []
            new_named_cmp = new_named or []
            if new_named_cmp != orig_named_list:
                corrections.append((
                    f"{stmt_key}.policies_named",
                    ", ".join(orig_named_list),
                    ", ".join(new_named_cmp),
                ))

    # Policy table
    original_policies = original_data.get("policies") or []
    new_policies = []
    for i, orig in enumerate(original_policies):
        new_pol = dict(orig)
        for pf in ("policy_type", "policy_number", "carrier",
                   "effective_date", "expiration_date",
                   "coverage_limits", "policy_basis",
                   "retro_date"):
            form_key = f"policy_{i}_{pf}"
            if form_key not in request.form:
                # Preserve original if not submitted
                continue
            submitted = (request.form.get(form_key) or "").strip() or None
            original_val = orig.get(pf)
            if isinstance(original_val, str):
                original_val = original_val.strip() or None
            if submitted != original_val:
                corrections.append((f"policies[{i}].{pf}", str(original_val or ""), str(submitted or "")))
            new_pol[pf] = submitted if submitted is not None else orig.get(pf)
        # Tri-state checkboxes (AI and WoS)
        for chk in ("additional_insured_checked", "waiver_of_subrogation_checked"):
            form_key = f"policy_{i}_{chk}"
            if form_key not in request.form:
                continue
            submitted = _norm_tristate(request.form.get(form_key))
            original = _norm_tristate(orig.get(chk))
            if submitted != original:
                corrections.append((f"policies[{i}].{chk}", original, submitted))
            new_pol[chk] = submitted
        # Boolean checkboxes (P&N, tail, policy_number_invalid)
        for bool_field in ("primary_noncontributory_checked",
                           "tail_coverage_evidenced",
                           "policy_number_invalid"):
            form_key = f"policy_{i}_{bool_field}"
            if form_key not in request.form:
                continue
            submitted_bool = request.form.get(form_key) == "true"
            original_bool = bool(orig.get(bool_field))
            if submitted_bool != original_bool:
                corrections.append((f"policies[{i}].{bool_field}",
                                     str(original_bool), str(submitted_bool)))
            new_pol[bool_field] = submitted_bool
        new_policies.append(new_pol)

    return corrections, new_policies


def _apply_form_to_data(new_data, original_data, new_policies):
    """Apply submitted form values (statement + top-level text + document_type) to new_data.

    Only overwrites fields that are present in the form submission — a missing form
    key leaves the original value intact.
    """
    new_data["policies"] = new_policies

    # Top-level text + document_type (only update if submitted)
    for field in _TOP_LEVEL_TEXT_FIELDS + ("document_type",):
        if field in request.form:
            submitted = (request.form.get(field) or "").strip() or None
            new_data[field] = submitted

    # Statement fields — only overwrite if submitted
    for stmt_key, form_prefix in _STATEMENT_FIELDS:
        if f"{form_prefix}_present" in request.form or f"{form_prefix}_quote" in request.form \
                or f"{form_prefix}_policies_named" in request.form:
            new_present = request.form.get(f"{form_prefix}_present") == "true"
            new_quote = (request.form.get(f"{form_prefix}_quote") or "").strip() or None
            raw_named = (request.form.get(f"{form_prefix}_policies_named") or "").strip()
            new_named = [p.strip() for p in raw_named.split(",") if p.strip()] or None
            new_data[stmt_key] = {
                "present": new_present,
                "quote": new_quote,
                "policies_named": new_named,
            }


_AIRTABLE_FIELD_NAMES = {
    "named_insured": "Named Insured",
    "certificate_holder": "Certificate Holder",
    "document_type": "Document Type",
}


def _handle_approve(table, record_id, source_filename, original_data, why_note, now_iso, required_policy_types=None):
    """Apply edits, write Corrections rows, set Review Status. Returns corrections count."""
    corrections, new_policies = _collect_corrections(original_data)

    # Write Corrections rows
    for field_path, old_val, new_val in corrections:
        _write_correction(source_filename, record_id, field_path, old_val, new_val, why_note, now_iso)

    review_status = REVIEW_STATUS_APPROVED_EDITED if corrections else REVIEW_STATUS_APPROVED

    update = {"Review Status": review_status, "Reviewed At": now_iso}
    if corrections:
        new_data = dict(original_data)
        _apply_form_to_data(new_data, original_data, new_policies)
        # Mirror edits to Airtable's top-level columns if the field maps directly
        for fp, _old, _new in corrections:
            if fp in _AIRTABLE_FIELD_NAMES:
                update[_AIRTABLE_FIELD_NAMES[fp]] = new_data.get(fp) or ""
        update["Raw JSON"] = json.dumps(new_data, indent=2)

    table.update(record_id, update, typecast=True)

    # Write reviewed-fields audit record
    policies = original_data.get("policies") or []
    reviewed_fields = ["named_insured", "contact_email"]
    if required_policy_types:
        for keywords, label in required_policy_types:
            pol_idx, _ = _find_key_policy(policies, keywords)
            if pol_idx is not None:
                reviewed_fields += [
                    f"policy_{pol_idx}_policy_number",
                    f"policy_{pol_idx}_effective_date",
                    f"policy_{pol_idx}_expiration_date",
                ]
            else:
                safe_lbl = label.lower().replace(" ", "_").replace("'", "")
                reviewed_fields.append(f"{safe_lbl}_not_found")
    audit_val = json.dumps({
        "reviewed_at": now_iso,
        "required_types": [lbl for _, lbl in (required_policy_types or [])],
        "fields_reviewed": reviewed_fields,
        "review_status": review_status,
    })
    _write_correction(source_filename, record_id, "reviewed_fields_audit",
                      "", audit_val, why_note, now_iso)

    if why_note:
        _write_note_record(source_filename, record_id, why_note, now_iso, corrections)

    # Vendor/VCA creation + schedule processor.py for RTR certs
    if _is_billy_rtr(source_filename):
        _trigger_post_confirm(record_id)

    return len(corrections)


def _handle_save_draft(table, record_id, source_filename, original_data, why_note, now_iso):
    """Save edits + write Corrections rows without changing Review Status."""
    corrections, new_policies = _collect_corrections(original_data)

    for field_path, old_val, new_val in corrections:
        _write_correction(source_filename, record_id, field_path, old_val, new_val, why_note, now_iso)

    if corrections:
        new_data = dict(original_data)
        _apply_form_to_data(new_data, original_data, new_policies)
        update = {"Raw JSON": json.dumps(new_data, indent=2)}
        for fp, _old, _new in corrections:
            if fp in _AIRTABLE_FIELD_NAMES:
                update[_AIRTABLE_FIELD_NAMES[fp]] = new_data.get(fp) or ""
        table.update(record_id, update, typecast=True)

    return len(corrections)


# ── Corrections page ─────────────────────────────────────────────────────────

@app.route("/corrections")
@login_required
def corrections_page():
    """Read-only view of all Corrections rows, with filters."""
    table = _corrections_table()
    field_filter = request.args.get("field", "").strip()
    model_filter = request.args.get("model", "").strip()
    status_filter = request.args.get("status", "").strip()

    all_records = table.all()
    rows = []
    fields_set = set()
    models_set = set()
    statuses_set = set()

    for rec in all_records:
        f = rec["fields"]
        fn = f.get("Field") or ""
        mo = f.get("Model") or ""
        st = f.get("Learning Status") or ""
        fields_set.add(fn)
        models_set.add(mo)
        statuses_set.add(st)
        if field_filter and fn != field_filter:
            continue
        if model_filter and mo != model_filter:
            continue
        if status_filter and st != status_filter:
            continue
        rows.append({
            "id": rec["id"],
            "summary": f.get("Summary") or "",
            "source_filename": f.get("Source Filename") or "",
            "record_id": f.get("Incoming Extraction Record ID") or "",
            "field": fn,
            "old_value": f.get("Old Value") or "",
            "new_value": f.get("New Value") or "",
            "why": f.get("Why (Haley's note)") or "",
            "model": mo,
            "corrected_at": f.get("Corrected At") or "",
            "status": st,
        })

    return render_template(
        "corrections.html",
        rows=rows,
        total=len(all_records),
        fields=sorted(fn for fn in fields_set if fn),
        models=sorted(mo for mo in models_set if mo),
        statuses=sorted(st for st in statuses_set if st),
        field_filter=field_filter,
        model_filter=model_filter,
        status_filter=status_filter,
    )


def _write_correction(source_filename, record_id, field_path, old_val, new_val, why_note, corrected_at):
    try:
        _corrections_table().create({
            "Summary": f"Changed {field_path}: '{old_val}' → '{new_val}'",
            "Source Filename": source_filename,
            "Incoming Extraction Record ID": record_id,
            "Field": field_path,
            "Old Value": str(old_val),
            "New Value": str(new_val),
            "Why (Haley's note)": why_note or "",
            "Model": EXTRACTION_MODEL,
            "Corrected At": corrected_at,
            "Learning Status": "New",
        })
    except Exception as exc:
        app.logger.warning("Failed to write Correction row for %s: %s", field_path, exc)


def _draft_prompt_rule(why_note: str, corrections: list) -> str:
    """Ask Claude Haiku for a concise extraction-prompt rule based on a Reading Correction."""
    field_lines = "\n".join(
        f"  Field: {fp}\n  Was: {old!r}\n  Should be: {new!r}"
        for fp, old, new in corrections
    ) or "  (note only, no specific field changes)"
    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": (
                "A human reviewer corrected an AI extraction of an insurance certificate. "
                "Write a short, precise instruction (1-3 sentences) to add to the extraction "
                "AI prompt to prevent this type of error in future certificates. "
                "Be specific about what to look for on the document.\n\n"
                f"Reviewer note: {why_note.strip()}\n\n"
                f"Field corrections:\n{field_lines}\n\n"
                "Proposed prompt rule (start directly with the instruction, no preamble):"
            )}],
        )
        return (resp.content[0].text or "").strip()
    except Exception as exc:
        app.logger.debug("Rule drafter failed (%s)", exc)
        return ""


def _draft_code_proposal(why_note: str) -> str:
    """Ask Claude Haiku to describe what code change would implement a Behavior Rule note."""
    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": (
                "A reviewer left this note while reviewing an insurance certificate in a compliance "
                "system. The note is classified as a 'Behavior Rule' — the system should change how "
                "it processes or displays something in general.\n\n"
                f"Note: {why_note.strip()}\n\n"
                "Describe in 2-4 sentences exactly what code change would implement this rule. "
                "Name the relevant file (review_app.py, processor.py, extractor.py, "
                "module_7b_requirement_validator.py, etc.) and the specific behavior to change. "
                "Be concrete.\n\n"
                "Proposed code change (start directly, no preamble):"
            )}],
        )
        return (resp.content[0].text or "").strip()
    except Exception as exc:
        app.logger.debug("Code proposal drafter failed (%s)", exc)
        return ""


def _classify_note(why_note: str) -> str:
    """Classify a reviewer note using Claude Haiku. Returns one of the Note Type select values."""
    if not why_note or len(why_note.strip()) < 8:
        return "One-off"

    _BEHAVIOR_KEYWORDS = (
        "should", "always", "never", "need to", "needs to", "must", "every time",
        "every cert", "all certs", "in general", "going forward", "add ", "show ",
        "display ", "flag ", "surface ", "track ", "save ", "store ", "record ",
        "feature", "important to notice", "red flag", "why is", "how do we",
        "change the", "update the", "fix the", "system should", "we should",
        "make sure", "want to see", "want to know", "i want", "we want",
        "need this", "would be helpful", "it would help",
    )
    _note_lower = why_note.strip().lower()
    _keyword_vote = any(kw in _note_lower for kw in _BEHAVIOR_KEYWORDS)

    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=16,
            messages=[{
                "role": "user",
                "content": (
                    "Classify this reviewer note. Reply with ONLY one label — nothing else.\n\n"
                    "Labels:\n"
                    "  Reading Correction — the AI misread or mis-extracted a specific value from this certificate "
                    "(wrong date, wrong number, wrong name). The fix is correcting the AI's reading of this document.\n"
                    "  Behavior Rule — the reviewer describes something the SYSTEM should always do differently: "
                    "show more info, flag something, track a field, change how data is displayed or processed. "
                    "Even if phrased as a question ('why doesn't it show X?'), if it implies the system should "
                    "behave differently in general, it is a Behavior Rule.\n"
                    "  One-off — purely specific to this one certificate with no broader implication "
                    "(e.g. 'this vendor called me', 'approved verbally', 'exception granted this time').\n\n"
                    "When in doubt between Behavior Rule and One-off, prefer Behavior Rule.\n\n"
                    "Examples:\n"
                    "  'The AI read the expiration as 2024 but it should be 2025' → Reading Correction\n"
                    "  'We should always show confidence scores so I know why it flagged something' → Behavior Rule\n"
                    "  'The image is cut off, we need to be able to scroll to see the cert holder box' → Behavior Rule\n"
                    "  'Contact emails showing are not on the cert, should not be used for vendor requests' → Behavior Rule\n"
                    "  'Warning text like not licensed should be a red flag on the review screen' → Behavior Rule\n"
                    "  'Billy called and said this vendor is approved for this job only' → One-off\n\n"
                    f"Note: {why_note.strip()}"
                ),
            }],
        )
        result = (resp.content[0].text or "").strip()
        if "Reading" in result:
            return "Reading Correction"
        if "Behavior" in result:
            return "Behavior Rule"
        # Model said One-off — override if keyword vote says Behavior Rule
        if _keyword_vote:
            return "Behavior Rule"
        return "One-off"
    except Exception as exc:
        app.logger.debug("Note classifier failed (%s) — keyword fallback", exc)
        return "Behavior Rule" if _keyword_vote else "One-off"


def _write_note_record(source_filename, record_id, why_note, corrected_at, corrections=None):
    """Classify the note, add reading corrections to the test set, draft a rule, and log everything."""
    note_type = _classify_note(why_note)
    field_corrections = [
        (fp, old, new)
        for fp, old, new in (corrections or [])
        if fp not in ("reviewed_fields_audit",)
    ]
    proposed = ""
    learning_status = "New"

    if note_type == "Reading Correction":
        # Add each field correction to the Extraction Training Log (test set)
        try:
            tlog = _training_log_table()
            if field_corrections:
                for fp, old_val, new_val in field_corrections:
                    tlog.create({
                        "Certificate ID": source_filename,
                        "Field Name": fp,
                        "Pipeline Value": str(old_val),
                        "Correct Value": str(new_val),
                        "Notes": why_note,
                        "Reviewer": "Haley",
                    })
            else:
                tlog.create({
                    "Certificate ID": source_filename,
                    "Field Name": "unspecified",
                    "Pipeline Value": "",
                    "Correct Value": "",
                    "Notes": why_note,
                    "Reviewer": "Haley",
                })
            app.logger.info("Added %d training-log entry(s) for %s",
                            max(1, len(field_corrections)), source_filename)
        except Exception as exc:
            app.logger.warning("Failed to add training log entry for %s: %s", source_filename, exc)

        # Draft a proposed prompt rule
        proposed = _draft_prompt_rule(why_note, field_corrections)
        if proposed:
            learning_status = "Proposed prompt rule"
            app.logger.info("Proposed prompt rule drafted for %s", source_filename)

    elif note_type == "Behavior Rule":
        proposed = _draft_code_proposal(why_note)
        app.logger.info("Code change proposal drafted for %s", source_filename)

    try:
        _corrections_table().create({
            "Summary": f"Note: {why_note[:80]}",
            "Source Filename": source_filename,
            "Incoming Extraction Record ID": record_id,
            "Field": "Note",
            "Old Value": proposed,
            "New Value": why_note,
            "Why (Haley's note)": why_note,
            "Model": EXTRACTION_MODEL,
            "Corrected At": corrected_at,
            "Learning Status": learning_status,
            "Note Type": note_type,
        })
        app.logger.info("Note classified as '%s' for %s", note_type, source_filename)
    except Exception as exc:
        app.logger.warning("Failed to write Note record for %s: %s", source_filename, exc)


def _get_weekly_learnings():
    """Return {note_type: [{note, src, record_id, proposed, learning_status}]} for Note records from last 7 days."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    try:
        safe_cutoff = cutoff.replace("'", "\\'")
        recs = _corrections_table().all(
            formula=f"AND({{Field}} = 'Note', IS_AFTER({{Corrected At}}, '{safe_cutoff}'))"
        )
        by_type = {"Reading Correction": [], "Behavior Rule": [], "One-off": []}
        for r in recs:
            f = r["fields"]
            nt = f.get("Note Type") or "One-off"
            note = f.get("New Value") or ""
            src = f.get("Source Filename") or ""
            proposed = f.get("Old Value") or ""
            ls = f.get("Learning Status") or "New"
            if note and nt in by_type:
                by_type[nt].append({
                    "note": note,
                    "src": src,
                    "record_id": r["id"],
                    "proposed": proposed,
                    "learning_status": ls,
                })
        return {k: v for k, v in by_type.items() if v}
    except Exception as exc:
        app.logger.warning("Failed to fetch weekly learnings: %s", exc)
        return {}


@app.route("/apply-reading-rule/<record_id>", methods=["POST"])
def apply_reading_rule(record_id):
    """Run the test suite; if tests pass, append the proposed rule to extractor.py CHECKBOX_RULES."""
    from flask import jsonify
    from pathlib import Path
    import subprocess, sys

    try:
        rec = _corrections_table().get(record_id)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Could not fetch record: {exc}"}), 404

    fields = rec.get("fields", {})
    proposed_rule = (fields.get("Old Value") or "").strip()
    learning_status = (fields.get("Learning Status") or "")
    if not proposed_rule or learning_status not in ("Proposed prompt rule", "New"):
        return jsonify({"ok": False, "error": "No pending proposed rule found on this record."}), 400

    # Gate: run the test suite
    test_result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q", "--tb=short"],
        capture_output=True, text=True,
        cwd=str(Path(__file__).parent),
        timeout=120,
    )
    if test_result.returncode != 0:
        output = (test_result.stdout + test_result.stderr)[-3000:]
        return jsonify({
            "ok": False,
            "error": "Test suite failed — rule NOT applied. Fix the failures first.",
            "test_output": output,
        })

    # Append rule to extractor.py CHECKBOX_RULES
    extractor_path = Path(__file__).parent / "extractor.py"
    marker = '"""\n\n_SYSTEM_PROMPT_TEMPLATE'
    try:
        src = extractor_path.read_text()
        if marker not in src:
            return jsonify({"ok": False, "error": "Could not find CHECKBOX_RULES end marker in extractor.py."}), 500
        new_src = src.replace(marker, f"- {proposed_rule}\n\"\"\"\n\n_SYSTEM_PROMPT_TEMPLATE", 1)
        extractor_path.write_text(new_src)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Failed to write extractor.py: {exc}"}), 500

    # Mark as applied
    try:
        _corrections_table().update(record_id, {"Learning Status": "Rule approved"})
    except Exception:
        pass

    return jsonify({"ok": True, "message": "Tests passed. Rule appended to extractor.py CHECKBOX_RULES."})


# ── Accept policy pattern ────────────────────────────────────────────────────

@app.route("/accept-policy-pattern/<extraction_id>", methods=["POST"])
@login_required
@csrf_protect
def accept_policy_pattern(extraction_id):
    from flask import jsonify
    norm_number = (request.form.get("norm_number") or "").strip()
    types_json  = (request.form.get("types_json") or "[]").strip()
    raw_number  = (request.form.get("raw_number") or "").strip()
    vendor_id   = (request.form.get("vendor_id") or "").strip()
    vendor_name = (request.form.get("vendor_name") or "").strip()

    if not norm_number or not vendor_id:
        abort(400)

    try:
        types_list = json.loads(types_json)
    except json.JSONDecodeError:
        types_list = []

    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    # Create Accepted Policy Patterns record
    app_tbl = api.table(base_id, ACCEPTED_POLICY_PATTERNS_TABLE_ID)
    app_tbl.create({
        "Vendor Name": vendor_name,
        "Vendor Record ID": vendor_id,
        "Policy Number Normalized": norm_number,
        "Coverage Types": json.dumps(types_list),
        "Policy Numbers Raw": raw_number,
        "Accepted At": datetime.now(timezone.utc).isoformat(),
    })

    # Remove this conflict from the extraction's Raw JSON
    ie_tbl = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)
    rec = ie_tbl.get(extraction_id)
    try:
        raw_json = json.loads(rec["fields"].get("Raw JSON") or "{}")
    except json.JSONDecodeError:
        raw_json = {}

    conflicts = raw_json.get("_shared_policy_conflicts") or []
    conflicts = [c for c in conflicts if c.get("norm_number") != norm_number]
    raw_json["_shared_policy_conflicts"] = conflicts

    update = {"Raw JSON": json.dumps(raw_json, indent=2)}
    if not conflicts:
        update["Review Status"] = "Approved"
    ie_tbl.update(extraction_id, update, typecast=True)

    return redirect(url_for("detail", record_id=extraction_id))


# ── Gaps ─────────────────────────────────────────────────────────────────────

def _parse_date(s):
    """Parse YYYY-MM-DD string to date, or None."""
    if not s:
        return None
    try:
        return datetime.strptime(str(s).strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _cadence_to_stage(cadence: str) -> str:
    """Map Gap Request Cadence Status to a Follow-ups pipeline stage."""
    c = cadence.lower()
    if "certificate received" in c:
        return "received"
    if "no response" in c or "due" in c:
        return "waiting"
    if c == "draft":
        return "drafted"
    if any(x in c for x in ("sent", "drafted", "call", "client update")):
        return "sent"
    return "missing"


def _stage_rank(cadence: str) -> int:
    return {"missing": 0, "drafted": 1, "sent": 2, "waiting": 3, "received": 4}.get(
        _cadence_to_stage(cadence), 0)


def _compute_date_gaps(policies_for_type: list, today: date) -> list:
    """Given a list of policy dicts (all same type) sorted by effective date,
    compute coverage gaps. Returns list of {gap_start, gap_end, days}.
    """
    # Sort by effective date
    dated = []
    for p in policies_for_type:
        eff = _parse_date(p.get("effective_date"))
        exp = _parse_date(p.get("expiration_date"))
        if eff and exp:
            dated.append((eff, exp, p))
    dated.sort(key=lambda x: x[0])
    if not dated:
        return []

    gaps = []
    # Check gap between consecutive terms
    for i in range(len(dated) - 1):
        _, exp_curr, _ = dated[i]
        eff_next, _, _ = dated[i + 1]
        if eff_next > exp_curr + timedelta(days=1):
            gap_days = (eff_next - exp_curr).days - 1
            gaps.append({
                "gap_start": exp_curr.isoformat(),
                "gap_end": eff_next.isoformat(),
                "days": gap_days,
            })

    # Check if latest policy is expired (no renewal)
    _, latest_exp, _ = dated[-1]
    if latest_exp < today:
        gaps.append({
            "gap_start": latest_exp.isoformat(),
            "gap_end": None,  # open-ended (no renewal on file)
            "days": (today - latest_exp).days,
            "expired": True,
        })

    return gaps


# ── Gap request / contact helpers ────────────────────────────────────────────

EMAIL_QUEUE_TABLE_ID = "tblCeRCf6RToTFkbL"

# Follow-up cadence days (configurable defaults)
_CADENCE_DAYS = (4, 8, 14, 21)


def _get_vendor_contact(vfields: dict) -> dict:
    """Return the best usable contact for a vendor."""
    agent_email  = (vfields.get("Agent Email") or "").strip()
    agent_name   = (vfields.get("Agent Name") or "").strip()
    agent_phone  = (vfields.get("Agent Phone") or "").strip()
    agent_agency = (vfields.get("Agent Agency") or "").strip()
    vendor_email = (vfields.get("Vendor Email") or "").strip()
    primary      = (vfields.get("Primary Contact") or "Agent").strip()
    bounced      = bool(vfields.get("Agent Bounced"))

    use_agent = agent_email and not bounced and (primary == "Agent" or not vendor_email)
    if use_agent:
        return {
            "type": "agent", "email": agent_email,
            "name": agent_name or "Insurance Agent",
            "phone": agent_phone, "agency": agent_agency,
        }
    if vendor_email:
        return {
            "type": "vendor", "email": vendor_email,
            "name": (vfields.get("Vendor Name") or "").strip(),
            "phone": "", "agency": "",
        }
    return {"type": "none", "email": "", "name": "", "phone": "", "agency": ""}


def _build_gap_request_email_body(
    contact: dict, client_name: str, cert_holder: str, subs: list,
    reply_to: str = "coi-intake@carolinacompliancesolutions.com",
) -> str:
    """Build a batched certificate request email covering multiple subs for one agent."""
    greeting = contact.get("name") or "Insurance Agent"
    agency_str = f" at {contact['agency']}" if contact.get("agency") else ""

    def _fmt_d(s):
        d = _parse_date(s)
        return d.strftime("%m/%d/%Y") if d else (s or "")

    lines = [
        f"Hello {greeting}{agency_str},",
        "",
        f"I am writing on behalf of {client_name} to request updated certificates of "
        f"insurance for the following subcontractors. Please reply to this email with "
        f"the updated certificates attached.",
        "",
    ]
    for sub in subs:
        lines.append(f"  {sub['vendor_name']}")
        lines.append(f"    Coverage needed: {sub['coverage_type']}")
        gs = sub.get("gap_start") or ""
        ge = sub.get("gap_end") or ""
        if gs or ge:
            lines.append(
                f"    Period not on file: {_fmt_d(gs) or 'an earlier date'} "
                f"through {_fmt_d(ge) or 'the present'}"
            )
        if sub.get("policy_number"):
            lines.append(f"    Known policy number: {sub['policy_number']}")
        if sub.get("vendor_email"):
            lines.append(f"    Subcontractor contact: {sub['vendor_email']}")
        lines.append("")

    lines += [
        f"Please issue the certificate with the following certificate holder:",
        f"  {cert_holder}",
        "",
        f"Certificates should be sent to {reply_to} and will be processed automatically.",
        "",
        "Thank you,",
        "Carolina Compliance Solutions",
        f"On behalf of {client_name}",
    ]
    return "\n".join(lines)


def _build_followup_email_body(
    day: int, contact: dict, client_name: str, cert_holder: str,
    subs: list, original_sent_date: str = "",
) -> str:
    """Build a follow-up email for the given cadence day."""
    greeting = contact.get("name") or "Insurance Agent"
    agency_str = f" at {contact['agency']}" if contact.get("agency") else ""

    def _fmt_d(s):
        d = _parse_date(s)
        return d.strftime("%m/%d/%Y") if d else (s or "")

    if day == 4:
        orig = ""
        if original_sent_date:
            d = _parse_date(original_sent_date)
            orig = f" (sent {d.strftime('%m/%d/%Y') if d else original_sent_date})"
        opener = (
            f"I wanted to follow up on our earlier certificate request{orig} "
            f"for {client_name} subcontractors."
        )
    elif day == 8:
        opener = (
            f"This is a second follow-up regarding outstanding certificates of insurance "
            f"for {client_name} subcontractors. We have not yet received the items below."
        )
    else:
        opener = f"We are following up on an outstanding certificate request for {client_name}."

    lines = [
        f"Hello {greeting}{agency_str},",
        "",
        opener,
        "",
    ]
    for sub in subs:
        lines.append(f"  {sub['vendor_name']} — {sub['coverage_type']}")
        gs = sub.get("gap_start") or ""
        ge = sub.get("gap_end") or ""
        if gs or ge:
            lines.append(
                f"    Period not on file: {_fmt_d(gs) or 'an earlier date'} "
                f"through {_fmt_d(ge) or 'the present'}"
            )
        lines.append("")

    if day == 8:
        lines += [
            "Could you please also copy the subcontractor on any reply so they are aware?",
            "",
        ]

    lines += [
        "Certificates should be sent to coi-intake@carolinacompliancesolutions.com.",
        "",
        "Thank you,",
        "Carolina Compliance Solutions",
        f"On behalf of {client_name}",
    ]
    return "\n".join(lines)


@app.route("/gaps")
@login_required
def gaps():
    from flask import jsonify
    client_id = request.args.get("client_id", RTR_CLIENT_ID)
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    today = date.today()
    window_start_str = request.args.get("window_start", "").strip()
    window_end_str   = request.args.get("window_end", "").strip()
    window_start = _parse_date(window_start_str)
    window_end   = _parse_date(window_end_str)
    has_window   = bool(window_start and window_end and window_start <= window_end)

    # Load all clients for the dropdown
    clients_tbl = api.table(base_id, CLIENTS_TABLE_ID)
    clients_raw = clients_tbl.all(fields=["Client Name"])
    clients = [
        {"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]}
        for r in clients_raw
    ]
    clients.sort(key=lambda c: c["name"])

    # Determine client name
    client_name = next((c["name"] for c in clients if c["id"] == client_id), client_id)

    # Get active vendor IDs for this client via VCA (uses client record's linked field)
    vendor_ids = _get_active_vendor_ids_for_client(client_id)

    # Load vendor details
    vendors_tbl = api.table(base_id, VENDORS_TABLE_ID)
    pol_tbl = api.table(base_id, INSURANCE_POLICIES_TABLE_ID)

    # Load existing gap requests for this client (fetch all, filter client-side)
    gap_tbl = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    try:
        gap_records = gap_tbl.all(
            fields=["Vendor", "Client", "Coverage Type", "Gap Start", "Gap End", "Status"],
        )
    except Exception:
        gap_records = []
    existing_requests_by_vendor: dict = defaultdict(list)
    for gr in gap_records:
        gf = gr["fields"]
        if client_id not in (gf.get("Client") or []):
            continue
        vlinks = gf.get("Vendor") or []
        for vid in vlinks:
            existing_requests_by_vendor[vid].append({
                "id": gr["id"],
                "coverage_type": gf.get("Coverage Type") or "",
                "gap_start": gf.get("Gap Start") or "",
                "gap_end": gf.get("Gap End") or "",
                "status": gf.get("Status") or "",
            })

    # Build per-vendor gap data
    vendors_out = []
    stat_missing = 0
    stat_date_gaps = 0
    stat_expiring = 0

    for vendor_id in vendor_ids:
        # Fetch vendor record
        vrec = {"fields": {}}
        try:
            vrec = vendors_tbl.get(vendor_id)
            vf = vrec["fields"]
            vendor_name   = vf.get("Vendor Name") or vendor_id
            contact_email = vf.get("Vendor Email") or ""
            agent_email   = vf.get("Agent Email") or ""
            agent_name    = vf.get("Agent Name") or ""
            agent_agency  = vf.get("Agent Agency") or ""
            agent_phone   = vf.get("Agent Phone") or ""
            agent_bounced = bool(vf.get("Agent Bounced"))
            primary_ct    = vf.get("Primary Contact") or "Agent"
        except Exception:
            vendor_name = vendor_id
            contact_email = agent_email = agent_name = ""
            agent_agency = agent_phone = primary_ct = ""
            agent_bounced = False

        # Fetch all policies for this vendor via vendor record link
        policy_ids = vrec["fields"].get("Insurance Policies") or []
        if not policy_ids:
            pol_records = []
        else:
            pol_formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in policy_ids) + ")"
            pol_records = pol_tbl.all(
                formula=pol_formula,
                fields=["Policy Type", "Policy Number", "Effective Date", "Expiration Date", "Carrier"],
            )

        # Group by coverage type (GL, WC)
        policies_by_type: dict = defaultdict(list)
        for prec in pol_records:
            pf = prec["fields"]
            pt = (pf.get("Policy Type") or "").lower()
            entry = {
                "policy_number": pf.get("Policy Number") or "",
                "effective_date": pf.get("Effective Date") or "",
                "expiration_date": pf.get("Expiration Date") or "",
                "carrier": pf.get("Carrier") or "",
            }
            if _pol_matches_audit_type(pt, _GL_KEYWORDS):
                policies_by_type["General Liability"].append(entry)
            elif any(k in pt for k in _WC_KEYWORDS):
                policies_by_type["Workers' Comp"].append(entry)

        vendor_gaps = []
        expiring_soon = []
        window_statuses = {}

        if has_window:
            for kws, label in RTR_REQUIRED_POLICY_TYPES:
                w_terms = []
                w_has_any = False
                for prec in pol_records:
                    pf = prec["fields"]
                    pt = (pf.get("Policy Type") or "").lower()
                    if not _pol_matches_audit_type(pt, kws):
                        continue
                    w_has_any = True
                    eff = _parse_date(pf.get("Effective Date"))
                    exp = _parse_date(pf.get("Expiration Date"))
                    if eff and exp and eff <= window_end and exp >= window_start:
                        w_terms.append({"effective": eff.isoformat(), "expiration": exp.isoformat()})
                w_terms.sort(key=lambda t: t["effective"])
                wstatus, wdays, _ = _compute_vendor_audit_status(
                    w_terms, window_start, window_end, w_has_any)
                window_statuses[label] = {"status": wstatus, "days_not_on_file": wdays}

        for _, label in RTR_REQUIRED_POLICY_TYPES:
            pols = policies_by_type.get(label) or []

            if not pols:
                vendor_gaps.append({
                    "type": label,
                    "kind": "missing",
                    "gap_start": None,
                    "gap_end": None,
                    "days": None,
                })
                stat_missing += 1
            else:
                # Check for expiring soon (within 30 days)
                for p in pols:
                    exp = _parse_date(p.get("expiration_date"))
                    if exp and today <= exp <= today + timedelta(days=30):
                        expiring_soon.append({
                            "type": label,
                            "expiration_date": p["expiration_date"],
                            "policy_number": p["policy_number"],
                        })
                        stat_expiring += 1

                # Compute date gaps
                dg = _compute_date_gaps(pols, today)
                for g in dg:
                    vendor_gaps.append({
                        "type": label,
                        "kind": "expired" if g.get("expired") else "date_gap",
                        "gap_start": g["gap_start"],
                        "gap_end": g.get("gap_end"),
                        "days": g["days"],
                    })
                    stat_date_gaps += 1

        has_window_issue = has_window and any(
            ws["status"] != "full" for ws in window_statuses.values()
        )
        has_usable_contact = bool(
            (agent_email and not agent_bounced) or contact_email
        )
        if vendor_gaps or expiring_soon or has_window_issue:
            vendors_out.append({
                "vendor_id": vendor_id,
                "vendor_name": vendor_name,
                "contact_email": contact_email,
                "agent_email": agent_email,
                "agent_name": agent_name,
                "agent_agency": agent_agency,
                "agent_phone": agent_phone,
                "agent_bounced": agent_bounced,
                "primary_contact": primary_ct,
                "has_usable_contact": has_usable_contact,
                "gaps": vendor_gaps,
                "expiring_soon": expiring_soon,
                "policies_by_type": {k: v for k, v in policies_by_type.items()},
                "existing_requests": existing_requests_by_vendor.get(vendor_id) or [],
                "window_statuses": window_statuses,
            })

    vendors_out.sort(key=lambda v: v["vendor_name"].lower())
    stat_no_contact = sum(1 for v in vendors_out if not v["has_usable_contact"])

    return render_template(
        "gaps.html",
        vendors=vendors_out,
        client_id=client_id,
        client_name=client_name,
        clients=clients,
        today=today.isoformat(),
        has_payments=False,
        stat_total_vendors=len(vendor_ids),
        stat_missing=stat_missing,
        stat_date_gaps=stat_date_gaps,
        stat_expiring=stat_expiring,
        stat_no_contact=stat_no_contact,
        has_window=has_window,
        window_start=window_start_str,
        window_end=window_end_str,
    )


@app.route("/gaps/draft-email/<vendor_id>", methods=["POST"])
@login_required
@csrf_protect
def gaps_draft_email(vendor_id):
    from flask import jsonify
    if _is_test_vendor(vendor_id):
        return jsonify({"ok": False, "error": "Test vendor — email drafts disabled"})
    client_id     = (request.form.get("client_id") or RTR_CLIENT_ID).strip()
    coverage_type = (request.form.get("coverage_type") or "").strip()
    gap_start     = (request.form.get("gap_start") or "").strip()
    gap_end       = (request.form.get("gap_end") or "").strip()

    # Look up vendor name
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    try:
        vrec = api.table(base_id, VENDORS_TABLE_ID).get(vendor_id)
        vendor_name = vrec["fields"].get("Vendor Name") or vendor_id
    except Exception:
        vendor_name = vendor_id

    # Format dates for display
    def _fmt(s):
        d = _parse_date(s)
        return d.strftime("%m/%d/%Y") if d else s

    gap_start_disp = _fmt(gap_start) if gap_start else "an earlier date"
    gap_end_disp   = _fmt(gap_end) if gap_end else "the present"

    email_body = (
        f"Hello,\n\n"
        f"We are reaching out on behalf of RTR Construction LLC regarding the "
        f"{coverage_type} insurance certificate for {vendor_name}.\n\n"
        f"Our records show that a {coverage_type} certificate is not on file for the period "
        f"{gap_start_disp} through {gap_end_disp}. Could you please provide an updated "
        f"certificate of insurance covering this period?\n\n"
        f"Please send the certificate to coi-intake@carolinacompliancesolutions.com at your earliest convenience.\n\n"
        f"Thank you,\nCarolina Compliance Solutions"
    )

    # Create Gap Request record
    gap_tbl = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    now_iso = datetime.now(timezone.utc).isoformat()
    new_rec = gap_tbl.create({
        "Vendor": [vendor_id],
        "Client": [client_id],
        "Coverage Type": coverage_type,
        "Gap Start": gap_start or None,
        "Gap End": gap_end or None,
        "Status": "Email Drafted",
        "Requested On": date.today().isoformat(),
        "Draft Email Body": email_body,
        "Created At": now_iso,
    })
    gap_id = new_rec["id"]

    # Determine contact for the Email Messages record
    try:
        vf_full = api.table(base_id, VENDORS_TABLE_ID).get(vendor_id)["fields"]
        contact_info = _get_vendor_contact(vf_full)
        to_email = contact_info["email"] or vf_full.get("Vendor Email") or ""
        to_source = contact_info["type"]
    except Exception:
        to_email = ""
        to_source = "vendor"

    subject_em = f"Certificate update needed — {vendor_name} — {coverage_type}"
    em_rec = _create_email_message(
        subject=subject_em, body=email_body,
        to_email=to_email, to_source=to_source,
        items_covered=f"{vendor_name} — {coverage_type}",
        vendor_ids=[vendor_id], client_ids=[client_id],
        gap_request_ids=[gap_id], now_iso=now_iso,
    )

    return jsonify({
        "ok": True,
        "draft": email_body,
        "request_id": gap_id,
        "em_id": em_rec.get("id") or "",
    })


@app.route("/gaps/draft-batch-requests", methods=["POST"])
@login_required
@csrf_protect
def gaps_draft_batch_requests():
    """Create one draft Email Queue record per agent, covering all their subs' open gaps."""
    from flask import jsonify
    client_id = (request.form.get("client_id") or RTR_CLIENT_ID).strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    today = date.today()

    try:
        crec = api.table(base_id, CLIENTS_TABLE_ID).get(client_id)
        cf = crec["fields"]
        client_name  = cf.get("Client Name") or "the client"
        cert_holder  = cf.get("Certificate Holder") or client_name
    except Exception:
        client_name = cert_holder = "the client"

    vendor_ids    = _get_active_vendor_ids_for_client(client_id)
    vendors_tbl   = api.table(base_id, VENDORS_TABLE_ID)
    pol_tbl       = api.table(base_id, INSURANCE_POLICIES_TABLE_ID)
    eq_tbl        = api.table(base_id, EMAIL_QUEUE_TABLE_ID)
    gap_tbl       = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    now_iso       = datetime.now(timezone.utc).isoformat()
    batch_id      = f"batch-{today.isoformat()}-{secrets.token_hex(3)}"

    # Collect open gap items per vendor
    agent_groups: dict = {}  # agent_email → {contact, subs[]}
    no_contact_vendors: list = []

    for vendor_id in vendor_ids:
        if _is_test_vendor(vendor_id):
            continue
        try:
            vrec  = vendors_tbl.get(vendor_id)
            vf    = vrec["fields"]
        except Exception:
            continue
        vendor_name  = vf.get("Vendor Name") or vendor_id
        contact      = _get_vendor_contact(vf)
        vendor_email = (vf.get("Vendor Email") or "").strip()

        policy_ids = vf.get("Insurance Policies") or []
        if policy_ids:
            pol_formula = "OR(" + ",".join(f"RECORD_ID()='{p}'" for p in policy_ids) + ")"
            pol_records = pol_tbl.all(
                formula=pol_formula,
                fields=["Policy Type", "Policy Number", "Effective Date", "Expiration Date"],
            )
        else:
            pol_records = []

        policies_by_type: dict = defaultdict(list)
        for prec in pol_records:
            pf  = prec["fields"]
            pt  = (pf.get("Policy Type") or "").lower()
            ent = {
                "policy_number":   pf.get("Policy Number") or "",
                "effective_date":  pf.get("Effective Date") or "",
                "expiration_date": pf.get("Expiration Date") or "",
            }
            if _pol_matches_audit_type(pt, _GL_KEYWORDS):
                policies_by_type["General Liability"].append(ent)
            elif any(k in pt for k in _WC_KEYWORDS):
                policies_by_type["Workers' Comp"].append(ent)

        v_gaps = []
        for _, label in RTR_REQUIRED_POLICY_TYPES:
            pols = policies_by_type.get(label) or []
            if not pols:
                v_gaps.append({
                    "vendor_name": vendor_name, "vendor_id": vendor_id,
                    "vendor_email": vendor_email, "coverage_type": label,
                    "gap_start": None, "gap_end": None, "policy_number": "",
                })
            else:
                dg = _compute_date_gaps(pols, today)
                for g in dg:
                    best_pol = max(
                        pols,
                        key=lambda p: _parse_date(p.get("expiration_date")) or date.min,
                    )
                    v_gaps.append({
                        "vendor_name": vendor_name, "vendor_id": vendor_id,
                        "vendor_email": vendor_email, "coverage_type": label,
                        "gap_start": g.get("gap_start"), "gap_end": g.get("gap_end"),
                        "policy_number": best_pol.get("policy_number") or "",
                    })

        if not v_gaps:
            continue

        if contact["type"] == "none":
            no_contact_vendors.append(vendor_name)
            continue

        ce = contact["email"]
        if ce not in agent_groups:
            agent_groups[ce] = {"contact": contact, "subs": []}
        agent_groups[ce]["subs"].extend(v_gaps)

    drafts_created = []
    for contact_email, grp in agent_groups.items():
        contact = grp["contact"]
        subs    = grp["subs"]
        n       = len(subs)

        subject = (
            f"Certificate request for {client_name} subcontractors — "
            f"{n} item{'s' if n != 1 else ''}"
        )
        body = _build_gap_request_email_body(
            contact=contact, client_name=client_name,
            cert_holder=cert_holder, subs=subs,
        )
        cc_list = list({
            s["vendor_email"] for s in subs
            if s["vendor_email"] and s["vendor_email"] != contact_email
        })

        eq_fields: dict = {
            "Primary Email":   contact_email,
            "Subject":         subject,
            "Body":            body,
            "Email Type":      "Gap Request",
            "Reminder Status": "Draft",
            "Record Status":   "Active",
            "Send After":      now_iso,
            "Created At":      now_iso,
            "Client":          [client_id],
        }
        if cc_list:
            eq_fields["CC Emails"] = ", ".join(cc_list)
        try:
            eq_rec = eq_tbl.create(eq_fields)
            eq_id  = eq_rec["id"]
        except Exception:
            eq_id = ""

        gap_ids = []
        for sub in subs:
            try:
                gr = gap_tbl.create({
                    "Vendor":         [sub["vendor_id"]],
                    "Client":         [client_id],
                    "Coverage Type":  sub["coverage_type"],
                    "Gap Start":      sub["gap_start"] or None,
                    "Gap End":        sub["gap_end"] or None,
                    "Status":         "Email Drafted",
                    "Requested On":   today.isoformat(),
                    "Draft Email Body": body,
                    "Contact Email":  contact_email,
                    "Contact Name":   contact.get("name") or "",
                    "Contact Phone":  contact.get("phone") or "",
                    "Batch ID":       batch_id,
                    "Cadence Status": "Draft",
                    "Created At":     now_iso,
                    "EQ IDs":         eq_id,
                })
                gap_ids.append(gr["id"])
            except Exception:
                pass

        # Also create an Email Messages draft record for Haley's copy-paste workflow
        vendor_ids_for_subs = list({s["vendor_id"] for s in subs if s.get("vendor_id")})
        items_text = "\n".join(
            f"{s['vendor_name']} — {s['coverage_type']}" for s in subs
        )
        em_rec = _create_email_message(
            subject=subject, body=body,
            to_email=contact_email,
            cc_email=", ".join(cc_list),
            to_source=contact.get("type") or "",
            items_covered=items_text,
            vendor_ids=vendor_ids_for_subs,
            client_ids=[client_id],
            gap_request_ids=gap_ids,
            now_iso=now_iso,
        )
        em_id = em_rec.get("id") or ""

        drafts_created.append({
            "contact_email": contact_email,
            "contact_name":  contact.get("name") or "",
            "contact_type":  contact.get("type"),
            "agency":        contact.get("agency") or "",
            "n_items":       n,
            "subs":          [s["vendor_name"] for s in subs],
            "subject":       subject,
            "body":          body,
            "eq_id":         eq_id,
            "gap_ids":       gap_ids,
            "em_id":         em_id,
        })

    return jsonify({
        "ok": True,
        "batch_id": batch_id,
        "drafts": drafts_created,
        "no_contact": no_contact_vendors,
    })


@app.route("/requests")
@login_required
def requests_page():
    client_id     = request.args.get("client_id", RTR_CLIENT_ID)
    status_filter = request.args.get("filter", "active")
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api     = _api()
    today   = date.today()

    clients_tbl = api.table(base_id, CLIENTS_TABLE_ID)
    clients     = sorted(
        [{"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]}
         for r in clients_tbl.all(fields=["Client Name"])],
        key=lambda c: c["name"],
    )
    client_name = next((c["name"] for c in clients if c["id"] == client_id), client_id)

    gap_tbl = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    try:
        all_reqs = gap_tbl.all(fields=[
            "Vendor", "Client", "Coverage Type", "Gap Start", "Gap End",
            "Status", "Cadence Status", "Contact Email", "Contact Name",
            "Contact Phone", "Batch ID", "Sent At", "Requested On",
            "EQ IDs", "Draft Email Body",
        ])
    except Exception:
        all_reqs = []

    vendors_tbl = api.table(base_id, VENDORS_TABLE_ID)
    _vname_cache: dict = {}

    def _vname(vid):
        if vid not in _vname_cache:
            try:
                _vname_cache[vid] = vendors_tbl.get(vid)["fields"].get("Vendor Name") or vid
            except Exception:
                _vname_cache[vid] = vid
        return _vname_cache[vid]

    rows = []
    for rec in all_reqs:
        rf = rec["fields"]
        if client_id not in (rf.get("Client") or []):
            continue

        cadence = rf.get("Cadence Status") or rf.get("Status") or "Draft"

        if status_filter == "active" and cadence in (
            "Closed — Certificate Received", "No Response", "Draft"
        ):
            continue
        if status_filter == "no_response" and cadence != "No Response":
            continue
        if status_filter == "drafts" and cadence != "Draft":
            continue
        if status_filter == "overdue":
            if "Due" not in cadence:
                continue

        vid_list    = rf.get("Vendor") or []
        vendor_name = _vname(vid_list[0]) if vid_list else "Unknown"
        sent_at     = rf.get("Sent At") or ""

        # Compute next action due date from sent_at + cadence
        cadence_map = {
            "Sent — Awaiting Reply": 4,
            "Day 4 Due": 4, "Day 4 Drafted": 8,
            "Day 8 Due": 8, "Day 8 Drafted": 14,
            "Day 14 — Call": 14,
            "Day 21 — Client Update": 21,
        }
        next_due = ""
        days_until = None
        if sent_at:
            d0 = _parse_date(sent_at)
            offset = cadence_map.get(cadence)
            if d0 and offset is not None:
                nd = d0 + timedelta(days=offset)
                next_due   = nd.isoformat()
                days_until = (nd - today).days

        rows.append({
            "id":            rec["id"],
            "vendor_name":   vendor_name,
            "coverage_type": rf.get("Coverage Type") or "",
            "gap_start":     rf.get("Gap Start") or "",
            "gap_end":       rf.get("Gap End") or "",
            "cadence_status": cadence,
            "contact_email": rf.get("Contact Email") or "",
            "contact_name":  rf.get("Contact Name") or "",
            "contact_phone": rf.get("Contact Phone") or "",
            "requested_on":  rf.get("Requested On") or "",
            "sent_at":       sent_at,
            "next_due":      next_due,
            "days_until":    days_until,
            "batch_id":      rf.get("Batch ID") or "",
            "draft_body":    rf.get("Draft Email Body") or "",
        })

    rows.sort(key=lambda r: (r["next_due"] or "9999-12-31"))

    stat_open        = sum(1 for r in rows if "Due" in r["cadence_status"] or "Awaiting" in r["cadence_status"])
    stat_overdue     = sum(1 for r in rows if r["days_until"] is not None and r["days_until"] < 0)
    stat_no_response = sum(1 for r in rows if r["cadence_status"] == "No Response")
    stat_draft       = sum(1 for r in rows if r["cadence_status"] == "Draft")

    return render_template(
        "requests.html",
        rows=rows,
        client_id=client_id,
        client_name=client_name,
        clients=clients,
        today=today.isoformat(),
        status_filter=status_filter,
        stat_open=stat_open,
        stat_overdue=stat_overdue,
        stat_no_response=stat_no_response,
        stat_draft=stat_draft,
    )


@app.route("/requests/<req_id>/mark-sent", methods=["POST"])
@login_required
@csrf_protect
def request_mark_sent(req_id):
    from flask import jsonify
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api     = _api()
    gap_tbl = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    eq_tbl  = api.table(base_id, EMAIL_QUEUE_TABLE_ID)
    today   = date.today()
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        rec = gap_tbl.get(req_id)
    except Exception:
        return jsonify({"ok": False, "error": "Not found"})

    rf          = rec["fields"]
    client_id   = (rf.get("Client") or [RTR_CLIENT_ID])[0]
    vid_list    = rf.get("Vendor") or []
    contact_email = rf.get("Contact Email") or ""
    contact_name  = rf.get("Contact Name") or ""
    contact_phone = rf.get("Contact Phone") or ""
    cov_type    = rf.get("Coverage Type") or ""

    vendor_name = vid_list[0] if vid_list else "vendor"
    if vid_list:
        try:
            vendor_name = api.table(base_id, VENDORS_TABLE_ID).get(
                vid_list[0])["fields"].get("Vendor Name") or vendor_name
        except Exception:
            pass

    try:
        client_name = api.table(base_id, CLIENTS_TABLE_ID).get(
            client_id)["fields"].get("Client Name") or "the client"
    except Exception:
        client_name = "the client"

    eq_ids_new = []
    for day in _CADENCE_DAYS:
        send_after = (today + timedelta(days=day)).isoformat()
        contact = {"name": contact_name, "email": contact_email,
                   "phone": contact_phone, "agency": ""}
        subs = [{
            "vendor_name": vendor_name, "coverage_type": cov_type,
            "gap_start": rf.get("Gap Start") or "",
            "gap_end":   rf.get("Gap End") or "",
            "vendor_email": "",
        }]

        if day == 14:
            to_email = "haley@carolinacompliancesolutions.com"
            subject  = (f"[Call needed] {vendor_name} — {cov_type} certificate outstanding")
            body     = (
                f"Please call the agent or subcontractor for {vendor_name} ({cov_type}).\n\n"
                f"Contact: {contact_name or '(see below)'}\n"
                f"Email:   {contact_email}\n"
                f"Phone:   {contact_phone or '(check certificate)'}\n\n"
                "Script:\n"
                f"\"Hi, I'm following up on a certificate of insurance request for "
                f"{vendor_name}. We need a {cov_type} certificate for {client_name}. "
                "Could you please send it to coi-intake@carolinacompliancesolutions.com?\"\n\n"
                "This is an internal reminder — do not send this email externally."
            )
            email_type = "Gap Request - Call Task"
        elif day == 21:
            to_email   = "haley@carolinacompliancesolutions.com"
            subject    = (f"[Client update draft] {vendor_name} — still no certificate")
            body       = (
                f"Hello,\n\n"
                f"We have not yet received a {cov_type} certificate from {vendor_name} "
                f"or their agent. We have made four outreach attempts and have not had a response.\n\n"
                "Please let us know how you would like to proceed.\n\n"
                "Carolina Compliance Solutions"
            )
            email_type = "Gap Request - Client Update"
        else:
            to_email   = contact_email
            body       = _build_followup_email_body(
                day=day, contact=contact, client_name=client_name,
                cert_holder=client_name, subs=subs,
                original_sent_date=today.isoformat(),
            )
            subject    = (
                f"Follow-up: Certificate request for {client_name} — {vendor_name}"
            )
            email_type = f"Gap Request Follow-up Day {day}"

        try:
            eq_rec = eq_tbl.create({
                "Primary Email":   to_email,
                "Subject":         subject,
                "Body":            body,
                "Email Type":      email_type,
                "Reminder Status": "Draft",
                "Record Status":   "Active",
                "Send After":      send_after,
                "Created At":      now_iso,
                "Client":          [client_id],
            })
            eq_ids_new.append(eq_rec["id"])
        except Exception:
            pass

    existing = rf.get("EQ IDs") or ""
    all_eq   = "\n".join(filter(None, [existing] + eq_ids_new))
    gap_tbl.update(req_id, {
        "Sent At":        today.isoformat(),
        "Status":         "Open",
        "Cadence Status": "Sent — Awaiting Reply",
        "EQ IDs":         all_eq,
    })
    return jsonify({"ok": True, "sent_at": today.isoformat(),
                    "followups_created": len(eq_ids_new)})


@app.route("/requests/<req_id>/close", methods=["POST"])
@login_required
@csrf_protect
def request_close(req_id):
    from flask import jsonify
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api     = _api()
    gap_tbl = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    eq_tbl  = api.table(base_id, EMAIL_QUEUE_TABLE_ID)

    try:
        rec = gap_tbl.get(req_id)
    except Exception:
        return jsonify({"ok": False, "error": "Not found"})

    eq_ids = [e.strip() for e in (rec["fields"].get("EQ IDs") or "").split("\n") if e.strip()]
    for eid in eq_ids:
        try:
            eq_tbl.update(eid, {"Record Status": "Cancelled", "Reminder Status": "Cancelled"})
        except Exception:
            pass

    gap_tbl.update(req_id, {
        "Status":         "Closed",
        "Cadence Status": "Closed — Certificate Received",
    })
    return jsonify({"ok": True})


# ── Emails ───────────────────────────────────────────────────────────────────

@app.route("/emails")
@login_required
def emails_page():
    from flask import jsonify
    filter_status = request.args.get("filter", "needs_sending")
    em_tbl = _em_table()
    try:
        all_msgs = em_tbl.all(fields=[
            "Subject", "Direction", "Status", "To", "CC", "Body",
            "Original Body", "Thread Key", "To Source", "Items Covered",
            "Sent At", "Received At", "Snoozed Until", "Edited At",
            "Vendor", "Client", "Linked Gap Request", "Linked Certificate",
        ])
    except Exception:
        all_msgs = []

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for rec in all_msgs:
        f = rec["fields"]
        status = f.get("Status") or "Draft"
        snoozed = f.get("Snoozed Until") or ""
        is_snoozed = bool(snoozed and snoozed > now_iso)

        if filter_status == "needs_sending" and not (status == "Draft" and not is_snoozed):
            continue
        if filter_status == "sent_waiting" and status != "Sent by Haley":
            continue
        if filter_status == "reply_received" and status != "Reply received":
            continue
        if filter_status == "no_response":
            if status != "Sent by Haley":
                continue
            # Treat as "no response" after 7 days
            sent = f.get("Sent At") or ""
            if not sent or (now_iso[:10] <= (datetime.fromisoformat(sent.replace("Z", "+00:00")) + timedelta(days=7)).date().isoformat()):
                continue

        rows.append({
            "id":            rec["id"],
            "subject":       f.get("Subject") or "",
            "status":        status,
            "direction":     f.get("Direction") or "Draft",
            "to_email":      f.get("To") or "",
            "cc_email":      f.get("CC") or "",
            "body":          f.get("Body") or "",
            "original_body": f.get("Original Body") or "",
            "thread_key":    f.get("Thread Key") or "",
            "to_source":     f.get("To Source") or "",
            "items_covered": f.get("Items Covered") or "",
            "sent_at":       f.get("Sent At") or "",
            "snoozed_until": snoozed,
            "is_snoozed":    is_snoozed,
        })

    counts = {
        "needs_sending": sum(1 for r in rows if r["status"] == "Draft" and not r["is_snoozed"]) if filter_status == "needs_sending" else _em_count_needs_sending(all_msgs, now_iso),
        "sent_waiting":  sum(1 for m in all_msgs if (m["fields"].get("Status") or "") == "Sent by Haley"),
        "reply_received": sum(1 for m in all_msgs if (m["fields"].get("Status") or "") == "Reply received"),
    }

    return render_template("emails.html", rows=rows, filter_status=filter_status, counts=counts)


def _em_count_needs_sending(all_msgs: list, now_iso: str) -> int:
    return sum(
        1 for m in all_msgs
        if (m["fields"].get("Status") or "") == "Draft"
        and not bool((m["fields"].get("Snoozed Until") or "") > now_iso)
    )


@app.route("/emails/create-problem-draft", methods=["POST"])
@login_required
@csrf_protect
def emails_create_problem_draft():
    """Create an Email Messages Draft for a certificate problem from the detail screen."""
    from flask import jsonify
    record_id    = (request.form.get("record_id") or "").strip()
    vendor_id    = (request.form.get("vendor_id") or "").strip()
    issue_summary = (request.form.get("issue_summary") or "").strip()
    now_iso = datetime.now(timezone.utc).isoformat()

    if not record_id:
        return jsonify({"ok": False, "error": "record_id required"})

    # Look up extraction for context
    table = _ie_table()
    try:
        rec = table.get(record_id)
        f = rec["fields"]
        named_insured = f.get("Named Insured") or ""
        source_filename = f.get("Source Filename") or ""
    except Exception:
        named_insured = ""
        source_filename = record_id

    # Look up vendor contact if we have vendor_id
    to_email = ""
    to_source = ""
    if vendor_id:
        try:
            vf = _api().table(clean_base_id(config.AIRTABLE_BASE_ID), VENDORS_TABLE_ID).get(vendor_id)["fields"]
            contact = _get_vendor_contact(vf)
            to_email = contact["email"]
            to_source = contact["type"]
        except Exception:
            pass

    subject = f"Certificate update needed — {named_insured}"
    body = (
        f"Hello,\n\n"
        f"We are following up regarding the certificate of insurance we received from {named_insured}.\n\n"
    )
    if issue_summary:
        body += f"{issue_summary}\n\n"
    body += (
        f"Please send an updated certificate to coi-intake@carolinacompliancesolutions.com.\n\n"
        f"Thank you,\nCarolina Compliance Solutions"
    )

    em_rec = _create_email_message(
        subject=subject, body=body,
        to_email=to_email, to_source=to_source,
        items_covered=source_filename,
        vendor_ids=[vendor_id] if vendor_id else [],
        extraction_ids=[record_id],
        now_iso=now_iso,
    )
    return jsonify({"ok": True, "em_id": em_rec.get("id") or "", "subject": em_rec.get("fields", {}).get("Subject") or subject})


@app.route("/emails/<em_id>/save", methods=["POST"])
@login_required
@csrf_protect
def emails_save(em_id):
    from flask import jsonify
    em_tbl = _em_table()
    now_iso = datetime.now(timezone.utc).isoformat()
    update: dict = {"Edited At": now_iso, "Edited By": "Haley"}
    for field, form_key in [("To", "to_email"), ("CC", "cc_email"), ("Subject", "subject"), ("Body", "body")]:
        val = request.form.get(form_key)
        if val is not None:
            update[field] = val.strip()
    try:
        em_tbl.update(em_id, update)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


@app.route("/emails/<em_id>/mark-sent", methods=["POST"])
@login_required
@csrf_protect
def emails_mark_sent(em_id):
    from flask import jsonify
    em_tbl = _em_table()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    now_iso = datetime.now(timezone.utc).isoformat()
    today = date.today()

    try:
        em_rec = em_tbl.get(em_id)
    except Exception:
        return jsonify({"ok": False, "error": "Not found"})

    ef = em_rec["fields"]
    # Update Email Messages record
    em_tbl.update(em_id, {
        "Status":    "Sent by Haley",
        "Direction": "Outbound",
        "Sent At":   now_iso,
    })

    # Update all linked Gap Requests to "Sent — Awaiting Reply" and start follow-up ladder
    gap_ids = ef.get("Linked Gap Request") or []
    gap_tbl = api.table(base_id, GAP_REQUESTS_TABLE_ID)
    eq_tbl  = api.table(base_id, EMAIL_QUEUE_TABLE_ID)

    for gap_id in gap_ids:
        try:
            gap_tbl.update(gap_id, {
                "Status":         "Email Sent",
                "Cadence Status": "Sent — Awaiting Reply",
                "Sent At":        today.isoformat(),
            })
        except Exception:
            pass

        # Queue follow-up EQ records for days 4, 8, 14, 21
        try:
            gap_rec  = gap_tbl.get(gap_id)
            gf       = gap_rec["fields"]
            contact_email = gf.get("Contact Email") or ef.get("To") or ""
            contact_name  = gf.get("Contact Name") or ""
            contact_phone = gf.get("Contact Phone") or ""
            cov_type      = gf.get("Coverage Type") or ""
            client_ids    = gf.get("Client") or []
            client_id     = client_ids[0] if client_ids else RTR_CLIENT_ID
            vendor_ids_gr = gf.get("Vendor") or []
            vendor_name   = vendor_ids_gr[0] if vendor_ids_gr else "vendor"
            if vendor_ids_gr:
                try:
                    vendor_name = api.table(base_id, VENDORS_TABLE_ID).get(vendor_ids_gr[0])["fields"].get("Vendor Name") or vendor_name
                except Exception:
                    pass
            client_name = "the client"
            try:
                client_name = api.table(base_id, CLIENTS_TABLE_ID).get(client_id)["fields"].get("Client Name") or client_name
            except Exception:
                pass

            contact = {"name": contact_name, "email": contact_email, "phone": contact_phone, "agency": ""}
            subs = [{
                "vendor_name": vendor_name, "coverage_type": cov_type,
                "gap_start": gf.get("Gap Start") or "", "gap_end": gf.get("Gap End") or "",
                "vendor_email": "",
            }]
            cert_holder = client_name

            eq_ids_new = []
            for day in _CADENCE_DAYS:
                send_after = (today + timedelta(days=day)).isoformat()
                if day == 14:
                    to_addr  = "haley@carolinacompliancesolutions.com"
                    subj_fu  = f"[Call needed] {vendor_name} — {cov_type} certificate outstanding"
                    body_fu  = f"Please call the agent for {vendor_name} ({cov_type}).\nContact: {contact_name}\nEmail: {contact_email}\nPhone: {contact_phone or '(check record)'}"
                    etype    = "Gap Request - Call Task"
                elif day == 21:
                    to_addr  = "haley@carolinacompliancesolutions.com"
                    subj_fu  = f"[Client update draft] {vendor_name} — still no certificate"
                    body_fu  = f"Draft a client update: {vendor_name} has not provided {cov_type} coverage after {today.strftime('%m/%d/%Y')}."
                    etype    = "Gap Request - Client Update"
                else:
                    to_addr  = contact_email
                    subj_fu  = f"Follow-up: certificate request — {vendor_name} — {cov_type}"
                    body_fu  = _build_followup_email_body(
                        day=day, contact=contact, client_name=client_name,
                        cert_holder=cert_holder, subs=subs,
                        original_sent_date=today.isoformat(),
                    )
                    etype    = "Gap Request Follow-up"

                eq_rec = eq_tbl.create({
                    "Primary Email":   to_addr,
                    "Subject":         subj_fu,
                    "Body":            body_fu,
                    "Email Type":      etype,
                    "Reminder Status": "Pending",
                    "Record Status":   "Active",
                    "Send After":      send_after,
                    "Created At":      now_iso,
                    "Client":          [client_id],
                })
                eq_ids_new.append(eq_rec["id"])

            if eq_ids_new:
                gap_tbl.update(gap_id, {"EQ IDs": "\n".join(eq_ids_new)})
        except Exception as exc:
            app.logger.warning("emails_mark_sent: follow-up creation failed for gap %s: %s", gap_id, exc)

    return jsonify({"ok": True, "sent_at": now_iso})


@app.route("/emails/<em_id>/dismiss", methods=["POST"])
@login_required
@csrf_protect
def emails_dismiss(em_id):
    from flask import jsonify
    try:
        _em_table().update(em_id, {"Status": "Closed"})
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


@app.route("/emails/<em_id>/snooze", methods=["POST"])
@login_required
@csrf_protect
def emails_snooze(em_id):
    from flask import jsonify
    days = request.form.get("days", "1")
    try:
        days = max(1, min(int(days), 30))
    except ValueError:
        days = 1
    snooze_until = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    try:
        _em_table().update(em_id, {"Snoozed Until": snooze_until})
        return jsonify({"ok": True, "snoozed_until": snooze_until})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


@app.route("/vendor/<vendor_id>/update-contact", methods=["POST"])
@login_required
@csrf_protect
def vendor_update_contact(vendor_id):
    from flask import jsonify
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api     = _api()

    fields_up = {}
    for key, airtable_field in [
        ("agent_name",    "Agent Name"),
        ("agent_email",   "Agent Email"),
        ("agent_phone",   "Agent Phone"),
        ("agent_agency",  "Agent Agency"),
        ("primary_contact", "Primary Contact"),
    ]:
        v = (request.form.get(key) or "").strip()
        if v:
            fields_up[airtable_field] = v

    bounced = request.form.get("agent_bounced")
    if bounced is not None:
        fields_up["Agent Bounced"] = bounced == "1"

    if fields_up:
        try:
            api.table(base_id, VENDORS_TABLE_ID).update(vendor_id, fields_up)
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    return jsonify({"ok": True})


# ── Audit ─────────────────────────────────────────────────────────────────────

_TEST_VENDOR_KEYWORDS = ("piedmont", "apex", "smith", "jones", "acme")


def _is_test_vendor(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in _TEST_VENDOR_KEYWORDS)


def _audit_coverage_keywords(audit_type: str):
    if "workers" in audit_type.lower() or "wc" in audit_type.lower() or "comp" in audit_type.lower():
        return _WC_KEYWORDS
    return _GL_KEYWORDS


def _audit_type_abbrev(audit_type: str) -> str:
    kws = _audit_coverage_keywords(audit_type)
    if kws is _WC_KEYWORDS:
        return "WC"
    return "GL"


def _pol_type_abbrev(pol_type: str) -> str:
    pt = pol_type.lower()
    if any(k in pt for k in _WC_KEYWORDS):
        return "WC"
    # Auto check BEFORE GL: "Commercial Auto Liability" must not match GL
    if "auto" in pt:
        return "Auto"
    if _pol_matches_audit_type(pt, _GL_KEYWORDS):
        return "GL"
    if "umbrella" in pt or "excess" in pt:
        return "Umb"
    return re.sub(r"[^A-Za-z0-9]", "", pol_type)[:4].upper() or "POL"


def _terms_for_vendor_audit(vendor_id: str, audit_type: str, start_date: date, end_date: date):
    """Return (terms, has_any_of_type).

    terms            — policy dicts that overlap the audit window for this coverage type.
    has_any_of_type  — True if the vendor has ANY policy of this type (even outside the window).
                       Used to distinguish 'none_in_window' from 'no_policy' in status.
    """
    kws = _audit_coverage_keywords(audit_type)
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    pol_tbl = api.table(base_id, INSURANCE_POLICIES_TABLE_ID)
    policy_ids = _get_vendor_policy_ids(vendor_id)
    if not policy_ids:
        return [], False
    pol_formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in policy_ids) + ")"
    records = pol_tbl.all(
        formula=pol_formula,
        fields=["Policy Type", "Policy Number", "Carrier", "Effective Date", "Expiration Date",
                "Last Updated From Extraction"],
    )
    terms = []
    has_any_of_type = False
    for rec in records:
        pf = rec["fields"]
        pt = (pf.get("Policy Type") or "").lower()
        if not _pol_matches_audit_type(pt, kws):
            continue
        has_any_of_type = True
        eff = _parse_date(pf.get("Effective Date"))
        exp = _parse_date(pf.get("Expiration Date"))
        if not eff or not exp:
            continue
        # Overlaps: effective <= end_date AND expiration >= start_date
        if eff <= end_date and exp >= start_date:
            terms.append({
                "record_id": rec["id"],
                "policy_number": pf.get("Policy Number") or "",
                "carrier": pf.get("Carrier") or "",
                "effective": eff.isoformat(),
                "expiration": exp.isoformat(),
                "extraction_ids": pf.get("Last Updated From Extraction") or [],
            })
    terms.sort(key=lambda t: t["effective"])
    return terms, has_any_of_type


def _compute_vendor_audit_status(terms: list, start_date: date, end_date: date,
                                  has_any: bool = True) -> tuple:
    """Return (status_str, days_not_on_file, gap_days_list).

    status values:
      'full'           — 100% covered
      'short_gap'      — gap ≤ 30 days
      'gap'            — gap > 30 days
      'none_in_window' — has policies of this type, but none overlap the window
      'no_policy'      — no policies of this type at all in the system
    """
    if not terms:
        total_days = (end_date - start_date).days + 1
        status = "none_in_window" if has_any else "no_policy"
        return status, total_days, []

    # Compute coverage union within [start_date, end_date]
    covered = set()
    for t in terms:
        eff = max(_parse_date(t["effective"]), start_date)
        exp = min(_parse_date(t["expiration"]), end_date)
        if eff <= exp:
            d = eff
            while d <= exp:
                covered.add(d)
                d += timedelta(days=1)

    total_days = (end_date - start_date).days + 1
    uncovered_days = total_days - len(covered)

    if uncovered_days == 0:
        return "full", 0, []

    # Find individual gaps
    gap_runs = []
    current_gap_start = None
    d = start_date
    while d <= end_date:
        if d not in covered:
            if current_gap_start is None:
                current_gap_start = d
        else:
            if current_gap_start is not None:
                gap_runs.append((current_gap_start, d - timedelta(days=1)))
                current_gap_start = None
        d += timedelta(days=1)
    if current_gap_start is not None:
        gap_runs.append((current_gap_start, end_date))

    max_gap = max((e - s).days + 1 for s, e in gap_runs) if gap_runs else 0

    if max_gap > 30:
        status = "gap"
    else:
        status = "short_gap"

    return status, uncovered_days, gap_runs


@app.route("/audit")
@login_required
def audit():
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    clients_tbl = api.table(base_id, CLIENTS_TABLE_ID)
    clients_raw = clients_tbl.all(fields=["Client Name"])
    clients = [
        {"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]}
        for r in clients_raw
    ]
    clients.sort(key=lambda c: c["name"])

    # Load recent audits (last 5)
    audit_tbl = api.table(base_id, AUDITS_TABLE_ID)
    try:
        recent_recs = audit_tbl.all(
            fields=["Audit Name", "Client", "Audit Type", "Start Date", "End Date", "Created At"],
            sort=["-Created At"],
            max_records=5,
        )
        recent_audits = []
        for rec in recent_recs:
            af = rec["fields"]
            recent_audits.append({
                "id": rec["id"],
                "name": af.get("Audit Name") or "",
                "client_ids": af.get("Client") or [],
                "audit_type": af.get("Audit Type") or "",
                "start_date": af.get("Start Date") or "",
                "end_date": af.get("End Date") or "",
                "created_at": af.get("Created At") or "",
            })
    except Exception:
        recent_audits = []

    return render_template("audit.html", clients=clients, recent_audits=recent_audits,
                           rtr_client_id=RTR_CLIENT_ID)


@app.route("/audit/preview", methods=["POST"])
@login_required
@csrf_protect
def audit_preview():
    from flask import jsonify
    client_id  = (request.form.get("client_id") or RTR_CLIENT_ID).strip()
    audit_type = (request.form.get("audit_type") or "General Liability").strip()
    start_str  = (request.form.get("start_date") or "").strip()
    end_str    = (request.form.get("end_date") or "").strip()

    start_date = _parse_date(start_str)
    end_date   = _parse_date(end_str)
    if not start_date or not end_date:
        return jsonify({"error": "Invalid date range"}), 400

    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    # Get vendor IDs for this client via active VCA records
    vendor_ids = _get_active_vendor_ids_for_client(client_id)

    vendors_tbl = api.table(base_id, VENDORS_TABLE_ID)

    counts = {"full": 0, "short_gap": 0, "gap": 0, "none_in_window": 0, "no_policy": 0}
    vendors_out = []

    for vendor_id in vendor_ids:
        try:
            vrec = vendors_tbl.get(vendor_id)
            vendor_name = vrec["fields"].get("Vendor Name") or vendor_id
        except Exception:
            vendor_name = vendor_id

        if _is_test_vendor(vendor_name):
            continue

        terms, has_any = _terms_for_vendor_audit(vendor_id, audit_type, start_date, end_date)
        status, days_not_on_file, gap_runs = _compute_vendor_audit_status(
            terms, start_date, end_date, has_any)

        counts[status] = counts.get(status, 0) + 1

        terms_out = []
        for t in terms:
            eff = _parse_date(t["effective"])
            days_before = (eff - start_date).days if eff and eff > start_date else 0
            terms_out.append({
                "policy_number": t["policy_number"],
                "carrier": t["carrier"],
                "effective": t["effective"],
                "expiration": t["expiration"],
                "days_before": max(0, days_before),
            })

        # Build gap_segments for display (e.g. "Sept 30 – Oct 22")
        gap_segments = []
        for gs, ge in gap_runs:
            gap_segments.append({
                "start": gs.isoformat(),
                "end": ge.isoformat(),
                "days": (ge - gs).days + 1,
            })

        vendors_out.append({
            "name": vendor_name,
            "vendor_id": vendor_id,
            "status": status,
            "has_any_of_type": has_any,
            "days_not_on_file": days_not_on_file,
            "terms": terms_out,
            "gap_segments": gap_segments,
        })

    vendors_out.sort(key=lambda v: v["name"].lower())

    # A5.1 — Unlinked pending extractions (RTR batch only)
    unlinked_pending = []
    if client_id == RTR_CLIENT_ID:
        try:
            ie_tbl = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)
            pending_recs = ie_tbl.all(
                formula=(
                    "AND({Review Status}='Pending Review', IS_BLANK({Matched Vendor}),"
                    "OR(FIND('COI_forms_',{Source Filename}),FIND('RTR_COI_',{Source Filename}),"
                    "FIND('N9WC',{Source Filename}),FIND('resend',LOWER({Source Filename}))))"
                ),
                fields=["Source Filename", "Named Insured", "Extraction Processed At"],
                max_records=50,
            )
            for rec in pending_recs:
                rf = rec["fields"]
                unlinked_pending.append({
                    "id": rec["id"],
                    "source_filename": rf.get("Source Filename") or "",
                    "named_insured": rf.get("Named Insured") or "",
                    "processed_at": rf.get("Extraction Processed At") or "",
                })
        except Exception:
            pass

    return jsonify({"vendors": vendors_out, "counts": counts,
                    "unlinked_pending": unlinked_pending})


@app.route("/audit/build", methods=["POST"])
@login_required
@csrf_protect
def audit_build():
    client_id      = (request.form.get("client_id") or RTR_CLIENT_ID).strip()
    audit_type     = (request.form.get("audit_type") or "General Liability").strip()
    start_str      = (request.form.get("start_date") or "").strip()
    end_str        = (request.form.get("end_date") or "").strip()
    carrier_filter = (request.form.get("carrier_filter") or "").strip()

    start_date = _parse_date(start_str)
    end_date   = _parse_date(end_str)
    if not start_date or not end_date:
        abort(400)

    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    # Get client name
    try:
        crec = api.table(base_id, CLIENTS_TABLE_ID).get(client_id)
        client_name = crec["fields"].get("Client Name") or client_id
    except Exception:
        client_name = client_id

    # Get vendors via active VCA records
    vendor_ids = _get_active_vendor_ids_for_client(client_id)

    vendors_tbl = api.table(base_id, VENDORS_TABLE_ID)
    ie_tbl = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)

    # Save audit record
    audit_name = f"{client_name} — {audit_type} — {start_str} to {end_str}"
    if carrier_filter:
        audit_name += f" ({carrier_filter})"
    try:
        api.table(base_id, AUDITS_TABLE_ID).create({
            "Audit Name": audit_name,
            "Client": [client_id],
            "Audit Type": audit_type,
            "Start Date": start_str,
            "End Date": end_str,
            "Carrier Filter": carrier_filter or None,
            "Created At": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        app.logger.warning("Failed to save audit record: %s", exc)

    # Build vendor data
    audit_vendors = []
    counts = {"full": 0, "short_gap": 0, "gap": 0, "none_in_window": 0, "no_policy": 0}

    for vendor_id in vendor_ids:
        try:
            vrec = vendors_tbl.get(vendor_id)
            vendor_name = vrec["fields"].get("Vendor Name") or vendor_id
        except Exception:
            vendor_name = vendor_id

        if _is_test_vendor(vendor_name):
            continue

        terms, has_any = _terms_for_vendor_audit(vendor_id, audit_type, start_date, end_date)
        status, days_not_on_file, _ = _compute_vendor_audit_status(
            terms, start_date, end_date, has_any)
        counts[status] = counts.get(status, 0) + 1

        kws = _audit_coverage_keywords(audit_type)
        pol_type_abbrev = _audit_type_abbrev(audit_type)

        # Gather PDFs via vendor's linked Incoming Extractions (approved only).
        # This replaces the old "Last Updated From Extraction" approach which
        # was rarely populated, resulting in nearly empty ZIPs.
        term_pdfs = []
        seen_urls: set = set()
        seen_extraction_ids: set = set()

        try:
            ext_ids = vrec["fields"].get("Incoming Extractions") or []
        except Exception:
            ext_ids = []

        for ext_id in ext_ids:
            if ext_id in seen_extraction_ids:
                continue
            try:
                ext_rec = ie_tbl.get(ext_id)
                ef = ext_rec["fields"]
                status_ext = (ef.get("Review Status") or "").strip()
                if status_ext not in ("Approved", "Approved + Edited", "Auto-Approved"):
                    continue
                # Check whether this extraction has a policy touching the audit window
                try:
                    raw_json = json.loads(ef.get("Raw JSON") or "{}")
                except Exception:
                    raw_json = {}
                ext_pols = raw_json.get("policies") or []
                has_matching_pol = False
                best_carrier = ""
                best_pol_num = ""
                best_eff = ""
                best_exp = ""
                for ep in ext_pols:
                    ep_pt = (ep.get("policy_type") or "").lower()
                    if not _pol_matches_audit_type(ep_pt, kws):
                        continue
                    eff = _parse_date(ep.get("effective_date") or "")
                    exp = _parse_date(ep.get("expiration_date") or "")
                    if eff and exp and eff <= end_date and exp >= start_date:
                        has_matching_pol = True
                        best_carrier = ep.get("carrier") or ""
                        best_pol_num = ep.get("policy_number") or ""
                        best_eff = ep.get("effective_date") or ""
                        best_exp = ep.get("expiration_date") or ""
                        break
                if not has_matching_pol:
                    seen_extraction_ids.add(ext_id)
                    continue
                # Prefer carrier/policy info from matching Insurance Policy term
                if terms:
                    best_carrier = best_carrier or terms[0]["carrier"]
                    best_pol_num = best_pol_num or terms[0]["policy_number"]
                    best_eff = best_eff or terms[0]["effective"]
                    best_exp = best_exp or terms[0]["expiration"]
                # Get Source Page attachments
                att_field = ef.get(SOURCE_PAGE_FIELD) or []
                if not isinstance(att_field, list):
                    att_field = []
                for att in att_field:
                    url = att.get("url") or ""
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        term_pdfs.append({
                            "url": url,
                            "carrier": best_carrier,
                            "policy_number": best_pol_num,
                            "effective": best_eff,
                            "expiration": best_exp,
                            "type_abbrev": pol_type_abbrev,
                        })
                seen_extraction_ids.add(ext_id)
            except Exception:
                pass

        audit_vendors.append({
            "vendor_id": vendor_id,
            "vendor_name": vendor_name,
            "status": status,
            "has_any_of_type": has_any,
            "days_not_on_file": days_not_on_file,
            "terms": terms,
            "pdfs": term_pdfs,
        })

    audit_vendors.sort(key=lambda v: v["vendor_name"].lower())

    # Build ZIP
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
        HAS_OPENPYXL = True
    except ImportError:
        HAS_OPENPYXL = False

    # Status row colors for Grid sheet
    _STATUS_FILLS = {
        "full":           "C6EFCE",  # green
        "short_gap":      "FFEB9C",  # yellow
        "gap":            "FFCC99",  # orange
        "none_in_window": "FFC7CE",  # red
        "no_policy":      "FFC7CE",  # red
    }

    def _xlsx_date(s: str) -> str:
        """Format YYYY-MM-DD as M/D/YYYY for Excel display."""
        d = _parse_date(s)
        if d:
            return f"{d.month}/{d.day}/{d.year}"
        return s or ""

    zip_buf = io.BytesIO()
    missing_files_lines: list = []

    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        if HAS_OPENPYXL:
            wb = openpyxl.Workbook()

            hdr_font = Font(name="Arial", bold=True, color="FFFFFF")
            hdr_fill = PatternFill("solid", fgColor="1F3864")  # dark navy
            arial_bold = Font(name="Arial", bold=True)
            arial = Font(name="Arial")

            def _apply_header(ws, row_num=1):
                for cell in ws[row_num]:
                    cell.font = hdr_font
                    cell.fill = hdr_fill
                    cell.alignment = Alignment(horizontal="center")

            # ── Summary sheet ──
            ws_sum = wb.active
            ws_sum.title = "Summary"
            ws_sum.append(["Audit Pack Summary"])
            ws_sum.append(["Client", client_name])
            ws_sum.append(["Audit Type", audit_type])
            ws_sum.append(["Period", f"{_xlsx_date(start_str)} to {_xlsx_date(end_str)}"])
            ws_sum.append(["Generated", datetime.now().strftime("%m/%d/%Y %H:%M")])
            ws_sum.append([])
            ws_sum.append(["30-day short-gap threshold: gaps ≤ 30 days are shown as 'Short Gap'"])
            ws_sum.append([])
            ws_sum.append(["Status", "Count"])
            _apply_header(ws_sum, row_num=9)
            for k, v in counts.items():
                ws_sum.append([k.replace("_", " ").title(), v])
            for row in ws_sum.iter_rows():
                for cell in row:
                    if cell.font == hdr_font:
                        continue
                    cell.font = arial

            # ── Grid sheet ──
            ws_grid = wb.create_sheet("Grid")
            ws_grid.append(["Sub Name", "Status", "Days Not On File"])
            _apply_header(ws_grid, row_num=1)
            for v in audit_vendors:
                row_idx = ws_grid.max_row + 1
                ws_grid.append([v["vendor_name"], v["status"].replace("_", " ").title(),
                                 v["days_not_on_file"]])
                fill_color = _STATUS_FILLS.get(v["status"])
                if fill_color:
                    row_fill = PatternFill("solid", fgColor=fill_color)
                    for cell in ws_grid[row_idx]:
                        cell.fill = row_fill
                        cell.font = arial
            for cell in ws_grid[1]:
                cell.font = hdr_font
                cell.fill = hdr_fill

            # ── Terms sheet ──
            ws_terms = wb.create_sheet("Terms")
            ws_terms.append(["Sub Name", "Coverage Type", "Policy Number", "Carrier",
                              "Effective", "Expiration", "Days Before Term"])
            _apply_header(ws_terms, row_num=1)
            for v in audit_vendors:
                for t in v["terms"]:
                    eff = _parse_date(t["effective"])
                    days_before = max(0, (eff - start_date).days) if eff and eff > start_date else 0
                    ws_terms.append([
                        v["vendor_name"], audit_type,
                        t["policy_number"], t["carrier"],
                        _xlsx_date(t["effective"]), _xlsx_date(t["expiration"]), days_before,
                    ])
                    for cell in ws_terms[ws_terms.max_row]:
                        cell.font = arial

            # ── Flags sheet ──
            ws_flags = wb.create_sheet("Flags")
            ws_flags.append(["Sub Name", "Issue"])
            _apply_header(ws_flags, row_num=1)
            for v in audit_vendors:
                if v["status"] in ("gap", "none_in_window", "no_policy"):
                    if v["status"] == "no_policy":
                        issue = f"No {audit_type} policy on file at all"
                    elif v["status"] == "none_in_window":
                        issue = f"No {audit_type} policy overlaps audit window"
                    else:
                        issue = f"{v['days_not_on_file']} day(s) not on file"
                    ws_flags.append([v["vendor_name"], issue])
                    for cell in ws_flags[ws_flags.max_row]:
                        cell.font = arial

            idx_buf = io.BytesIO()
            wb.save(idx_buf)
            zf.writestr("00 Index.xlsx", idx_buf.getvalue())

        # ── Per-vendor folders ──
        for v in audit_vendors:
            folder = re.sub(r'[\\/:*?"<>|]', "_", v["vendor_name"])
            if not v["terms"]:
                if v.get("has_any_of_type"):
                    note = f"{v['vendor_name']} — {audit_type} policy on file but does not overlap this audit window.\n"
                else:
                    note = f"{v['vendor_name']} — no {audit_type} policies on file.\n"
                zf.writestr(f"{folder}/Not on file.txt", note)
                continue

            if not v["pdfs"]:
                # Has terms but no PDFs (e.g. extraction not found or not approved)
                zf.writestr(
                    f"{folder}/No certificate PDFs found.txt",
                    (
                        f"{v['vendor_name']} — {audit_type} policy found in system "
                        f"but no approved certificate PDF located.\n"
                        f"Terms on file:\n"
                        + "\n".join(
                            f"  {t['policy_number']} ({t['carrier']}) "
                            f"{t['effective']} to {t['expiration']}"
                            for t in v["terms"]
                        )
                        + "\n"
                    ),
                )
                continue

            for seq, pdf_info in enumerate(v["pdfs"], start=1):
                raw_name = (
                    f"{seq:02d} - {v['vendor_name']} - {pdf_info['type_abbrev']} - "
                    f"{pdf_info['carrier']} - {pdf_info['policy_number']} - "
                    f"{pdf_info['effective']} to {pdf_info['expiration']}.pdf"
                )
                # Sanitize filename
                safe_name = re.sub(r'[\\/:*?"<>|]', "_", raw_name)
                url = pdf_info["url"]
                try:
                    req = urllib.request.Request(url, headers={"User-Agent": "CCS-Audit/1.0"})
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        pdf_data = resp.read()
                    zf.writestr(f"{folder}/{safe_name}", pdf_data)
                except Exception as exc:
                    app.logger.warning("Could not download %s: %s", url, exc)
                    err_msg = f"{v['vendor_name']}: {url} — {exc}"
                    missing_files_lines.append(err_msg)

        if missing_files_lines:
            zf.writestr(
                "MISSING FILES.txt",
                "The following certificate PDFs could not be downloaded:\n\n"
                + "\n".join(missing_files_lines)
                + "\n",
            )

    # Format dates in ZIP filename as M-D-YYYY
    def _zip_date(s: str) -> str:
        d = _parse_date(s)
        if d:
            return f"{d.month}-{d.day}-{d.year}"
        return s or ""

    zip_buf.seek(0)
    safe_client = re.sub(r'[\\/:*?"<>|]', "_", client_name)
    safe_type   = re.sub(r'[\\/:*?"<>|]', "_", audit_type)
    filename = f"{safe_client} - {safe_type} - {_zip_date(start_str)} to {_zip_date(end_str)}.zip"

    from flask import send_file
    return send_file(
        zip_buf,
        as_attachment=True,
        download_name=filename,
        mimetype="application/zip",
    )


# ── Payments ──────────────────────────────────────────────────────────────────

@app.route("/payments/import", methods=["GET", "POST"])
@login_required
def payments_import():
    from flask import jsonify
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    clients_tbl = api.table(base_id, CLIENTS_TABLE_ID)
    clients_raw = clients_tbl.all(fields=["Client Name"])
    clients = [
        {"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]}
        for r in clients_raw
    ]
    clients.sort(key=lambda c: c["name"])

    # Count recent sub payments
    try:
        pay_recs = api.table(base_id, SUB_PAYMENTS_TABLE_ID).all(max_records=1000)
        recent_count = len(pay_recs)
    except Exception:
        recent_count = 0

    if request.method == "GET":
        return render_template("payments_import.html", clients=clients, recent_count=recent_count)

    # POST: handle file upload
    if request.method == "POST":
        if "csv_file" not in request.files:
            return render_template("payments_import.html", clients=clients,
                                   recent_count=recent_count, error="No file uploaded.")

        client_id = (request.form.get("client_id") or RTR_CLIENT_ID).strip()
        csv_file = request.files["csv_file"]
        if not csv_file.filename:
            return render_template("payments_import.html", clients=clients,
                                   recent_count=recent_count, error="No file selected.")

        # Load vendor names for fuzzy matching
        vendors_tbl = api.table(base_id, VENDORS_TABLE_ID)
        vendor_recs = vendors_tbl.all(fields=["Vendor Name"])
        vendor_list = [
            {"id": r["id"], "name": r["fields"].get("Vendor Name") or "",
             "norm": _norm_ni(r["fields"].get("Vendor Name") or "")}
            for r in vendor_recs
        ]

        # Parse CSV
        try:
            content = csv_file.read().decode("utf-8-sig", errors="replace")
            reader = csv.DictReader(io.StringIO(content))
            rows_raw = list(reader)
        except Exception as exc:
            return render_template("payments_import.html", clients=clients,
                                   recent_count=recent_count, error=f"Could not parse CSV: {exc}")

        # Detect column names (flexible)
        if not rows_raw:
            return render_template("payments_import.html", clients=clients,
                                   recent_count=recent_count, error="CSV appears to be empty.")

        sample = rows_raw[0]
        headers_lower = {k.lower().strip(): k for k in sample.keys()}

        def _find_col(*candidates):
            for c in candidates:
                if c in headers_lower:
                    return headers_lower[c]
            return None

        date_col   = _find_col("date", "txn date", "transaction date")
        payee_col  = _find_col("payee", "name", "vendor", "payee name")
        amount_col = _find_col("amount", "debit", "credit", "total")

        results = []
        unmatched = []
        batch = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for row in rows_raw:
            payee  = (row.get(payee_col) or "").strip() if payee_col else ""
            amount = (row.get(amount_col) or "").strip() if amount_col else ""
            txn_date = (row.get(date_col) or "").strip() if date_col else ""

            if not payee and not amount:
                continue

            norm_payee = _norm_ni(payee)
            # Fuzzy match: find vendor with highest token overlap
            best_vendor = None
            best_score = 0
            payee_tokens = set(norm_payee.split())
            for v in vendor_list:
                if not v["norm"]:
                    continue
                v_tokens = set(v["norm"].split())
                if not v_tokens:
                    continue
                overlap = len(payee_tokens & v_tokens)
                score = overlap / max(len(payee_tokens | v_tokens), 1)
                if score > best_score and overlap >= 1:
                    best_score = score
                    best_vendor = v

            matched = best_vendor and best_score >= 0.3

            result_row = {
                "payee": payee,
                "matched_vendor": best_vendor["name"] if matched else "",
                "matched_vendor_id": best_vendor["id"] if matched else "",
                "date": txn_date,
                "amount": amount,
                "status": "Matched" if matched else "Unmatched",
            }
            results.append(result_row)
            if not matched:
                unmatched.append(payee)

            # Build Airtable record
            pay_rec = {
                "Client": client_id,
                "Vendor Name QB": payee,
                "Payment Date": txn_date or None,
                "Amount": float(re.sub(r"[^0-9.\-]", "", amount)) if amount else None,
                "Source File": csv_file.filename,
                "Match Status": "Matched" if matched else "Unmatched",
                "Imported At": now_iso,
            }
            if matched:
                pay_rec["Vendor"] = best_vendor["id"]
            batch.append(pay_rec)

            # Flush batch in groups of 10
            if len(batch) >= 10:
                try:
                    api.table(base_id, SUB_PAYMENTS_TABLE_ID).batch_create(batch)
                except Exception as exc:
                    app.logger.warning("Failed to create sub-payment batch: %s", exc)
                batch = []

        if batch:
            try:
                api.table(base_id, SUB_PAYMENTS_TABLE_ID).batch_create(batch)
            except Exception as exc:
                app.logger.warning("Failed to create final sub-payment batch: %s", exc)

        return render_template(
            "payments_import.html",
            clients=clients,
            recent_count=recent_count,
            results=results,
            unmatched=sorted(set(unmatched)),
            matched_count=sum(1 for r in results if r["status"] == "Matched"),
        )


# ── Pulse schema self-check ───────────────────────────────────────────────────
_PULSE_SCHEMA_CACHE: dict | None = None
_PULSE_SCHEMA_CACHE_TS: float = 0.0
_PULSE_FIELDS_USED: list = [
    ("Incoming Documents",        ["Subject", "Sender Email", "Date Received", "Status", "Skip Reason"]),
    ("Incoming Extractions",      ["Processing Status", "Review Status", "Source Filename"]),
    ("Insurance Certificates",    ["Certificate Name"]),
    ("Insurance Policies",        ["Policy Record"]),
    ("Vendors",                   ["Vendor Name", "Vendor Email"]),
    ("Clients",                   ["Client Name"]),
    ("Client Requirements",       ["Client Link"]),
    ("Vendor Client Assignments", ["Client Link"]),
    ("Gap Requests",              ["Request ID"]),
]

def _pulse_schema_mismatches(base_id: str, api_key: str) -> list:
    """Check fields used by Pulse against live schema; cached 1hr."""
    global _PULSE_SCHEMA_CACHE, _PULSE_SCHEMA_CACHE_TS
    import time as _t
    now = _t.time()
    if _PULSE_SCHEMA_CACHE is None or now - _PULSE_SCHEMA_CACHE_TS > 3600:
        try:
            import requests as _req
            resp = _req.get(
                f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=8,
            )
            resp.raise_for_status()
            data = resp.json()
            _PULSE_SCHEMA_CACHE = {
                t["name"]: {f["name"] for f in t.get("fields", [])}
                for t in data.get("tables", [])
            }
            _PULSE_SCHEMA_CACHE_TS = now
        except Exception:
            return []
    mismatches = []
    for tname, fields in _PULSE_FIELDS_USED:
        tbl_fields = _PULSE_SCHEMA_CACHE.get(tname) if _PULSE_SCHEMA_CACHE else None
        if tbl_fields is not None:
            for fname in fields:
                if fname not in tbl_fields:
                    mismatches.append({"table": tname, "field": fname})
    return mismatches


# ── Pulse page ────────────────────────────────────────────────────────────────

@app.route("/pulse")
@login_required
def pulse():
    """System Pulse page — live health dashboard."""
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api     = _api()

    def _now_iso():
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def _level_max(*levels):
        order = {"ok": 0, "warn": 1, "error": 2}
        return max(levels, key=lambda l: order.get(l, 0))

    # 1. Airtable space
    TABLE_PRIMARY_FIELDS = {
        "Incoming Documents": "Sender Email",
        "Incoming Extractions": "Source Filename",
        "Insurance Policies": "Policy Record",
        "Insurance Certificates": "Certificate Name",
        "Vendors": "Vendor Name",
        "Clients": "Client Name",
        "Email Queue": "Primary Email",
        "Compliance Log": "Log Entry",
        "Vendor Client Assignments": "Vendor Client Assignment",
        "Client Requirements": "Id",
    }
    airtable_space = {"total_records": None, "limit": 50_000, "level": "ok", "tables": [], "fastest_growing": None, "error_msg": None}
    try:
        table_names = [
            "Incoming Documents", "Incoming Extractions", "Insurance Policies",
            "Insurance Certificates", "Vendors", "Clients", "Email Queue",
            "Compliance Log", "Vendor Client Assignments", "Client Requirements",
        ]
        rows = []
        total = 0
        for tname in table_names:
            try:
                tbl = api.table(base_id, tname)
                count = len(tbl.all(fields=[TABLE_PRIMARY_FIELDS.get(tname, "Vendor Name")]))
                total += count
                rows.append({"name": tname, "count": count, "growth_7d": 0})
            except Exception:
                rows.append({"name": tname, "count": 0, "growth_7d": 0})
        rows.sort(key=lambda r: r["count"], reverse=True)
        airtable_space["total_records"] = total
        airtable_space["tables"] = rows[:5]
        if rows:
            airtable_space["fastest_growing"] = rows[0]["name"]
        pct = total / airtable_space["limit"]
        if pct >= 0.80:
            airtable_space["level"] = "error"
        elif pct >= 0.60:
            airtable_space["level"] = "warn"
    except Exception as exc:
        app.logger.warning("pulse airtable_space error: %s", exc)
        airtable_space["level"] = "error"
        airtable_space["error_msg"] = str(exc)[:120]

    # 2. Email intake
    today_str = date.today().isoformat()
    email_intake = {
        "last_check": None, "last_received": None,
        "today_received": 0, "today_processed": 0, "today_skipped": 0,
        "skipped_items": [], "level": "ok", "error_msg": None,
    }
    try:
        doc_tbl = api.table(base_id, "Incoming Documents")
        all_docs = doc_tbl.all(fields=["Subject", "Sender Email", "Date Received", "Status", "Skip Reason"])
        last_recv_ts = None
        skipped = []
        for rec in all_docs:
            f = rec["fields"]
            received_raw = (f.get("Date Received") or "").strip()
            if not received_raw:
                continue
            try:
                ts = datetime.fromisoformat(received_raw.replace("Z", "+00:00"))
                if last_recv_ts is None or ts > last_recv_ts:
                    last_recv_ts = ts
            except ValueError:
                pass
            if not received_raw.startswith(today_str):
                continue
            email_intake["today_received"] += 1
            status = (f.get("Status") or "").strip()
            if status in ("Skipped", "Not needed", "Not a COI"):
                email_intake["today_skipped"] += 1
                skipped.append({"id": rec["id"], "subject": f.get("Subject") or "(no subject)",
                                 "sender": f.get("Sender Email") or "", "received_at": received_raw,
                                 "reason": f.get("Skip Reason") or status})
            elif status:
                email_intake["today_processed"] += 1
        email_intake["skipped_items"] = skipped
        if last_recv_ts:
            email_intake["last_received"] = last_recv_ts.strftime("%Y-%m-%d %H:%M UTC")
        last_hb = _hs.get_last_heartbeat("pulse_cron")
        if last_hb:
            email_intake["last_check"] = datetime.fromtimestamp(last_hb, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if email_intake["today_skipped"] > 5:
            email_intake["level"] = "error"
        elif email_intake["today_skipped"] > 0:
            email_intake["level"] = "warn"
    except Exception as exc:
        app.logger.warning("pulse email_intake error: %s", exc)
        email_intake["level"] = "error"
        email_intake["error_msg"] = str(exc)[:120]

    # 3. Pipeline stage counts
    pipeline = {
        "by_stage": {"received": 0, "extracted": 0, "pending_review": 0, "confirmed": 0},
        "oldest_pending_days": None, "avg_days_received_to_confirmed": None,
        "stuck": [], "level": "ok", "error_msg": None,
    }
    try:
        ie_tbl = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)
        recs = ie_tbl.all(fields=["Processing Status", "Review Status", "Source Filename"])
        now_utc = datetime.now(tz=timezone.utc)
        oldest_pending = None
        stuck = []
        for rec in recs:
            f = rec["fields"]
            status = (f.get("Processing Status") or "").strip().lower()
            if status in ("received", "new", ""):
                pipeline["by_stage"]["received"] += 1
            elif status in ("extracted", "imported"):
                pipeline["by_stage"]["extracted"] += 1
            elif "pending" in status or "review" in status:
                pipeline["by_stage"]["pending_review"] += 1
                created_raw = (rec.get("createdTime") or "").strip()
                if created_raw:
                    try:
                        created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                        age_days = (now_utc - created_dt).days
                        if oldest_pending is None or age_days > oldest_pending:
                            oldest_pending = age_days
                    except ValueError:
                        pass
            elif status in ("confirmed", "approved", "processed"):
                pipeline["by_stage"]["confirmed"] += 1
        pipeline["oldest_pending_days"] = oldest_pending
        pipeline["stuck"] = stuck[:5]
        if oldest_pending is not None and oldest_pending > 7:
            pipeline["level"] = "error"
        elif oldest_pending is not None and oldest_pending > 3:
            pipeline["level"] = "warn"
    except Exception as exc:
        app.logger.warning("pulse pipeline error: %s", exc)
        pipeline["level"] = "error"
        pipeline["error_msg"] = str(exc)[:120]

    # 4. Writes & errors
    writes_errors = {"write_failures_24h": [], "ai_errors_24h": [], "level": "ok", "error_msg": None}
    try:
        all_events = _hs.get_recent_events(limit=200, since_hours=24)
        write_failures = [
            {"ts": e["ts"], "table": e.get("table_name") or "", "error": e["body"] or e["title"]}
            for e in all_events if e.get("category") in ("write_error", "airtable_write") and e.get("level") == "error"
        ]
        ai_errors = [
            {"ts": e["ts"], "error": e["title"], "retried": "retry" in (e.get("body") or "").lower()}
            for e in all_events if e.get("category") == "api_error" and e.get("level") == "error"
        ]
        writes_errors["write_failures_24h"] = write_failures
        writes_errors["ai_errors_24h"] = ai_errors
        if write_failures or ai_errors:
            writes_errors["level"] = "error" if len(write_failures) > 3 else "warn"
    except Exception as exc:
        app.logger.warning("pulse writes_errors error: %s", exc)
        writes_errors["level"] = "error"
        writes_errors["error_msg"] = str(exc)[:120]

    # 5. Scheduled jobs
    scheduled_jobs = []
    try:
        job_specs = [
            {"name": "Email intake poll",    "service": "email_monitor", "expected_interval_minutes": 15,   "result_ok": "Polled"},
            {"name": "Daily pipeline",       "service": "run_pipeline",  "expected_interval_minutes": 1440, "result_ok": "Ran"},
            {"name": "Pulse cron",           "service": "pulse_cron",    "expected_interval_minutes": 15,   "result_ok": "Checked"},
            {"name": "Requirement validator","service": "pipeline",      "expected_interval_minutes": 15,   "result_ok": "Ran"},
        ]
        now_ts = datetime.now(tz=timezone.utc).timestamp()
        for spec in job_specs:
            last_hb = _hs.get_last_heartbeat(spec["service"])
            last_run_str = None
            result_str = "No data"
            level = "warn"
            if last_hb:
                last_run_dt = datetime.fromtimestamp(last_hb, tz=timezone.utc)
                last_run_str = last_run_dt.strftime("%Y-%m-%d %H:%M UTC")
                age_minutes = (now_ts - last_hb) / 60
                max_minutes = spec["expected_interval_minutes"] * 1.5
                if age_minutes <= max_minutes:
                    result_str = spec["result_ok"]
                    level = "ok"
                else:
                    result_str = f"Overdue ({int(age_minutes)}m ago)"
                    level = "error" if age_minutes > max_minutes * 2 else "warn"
            scheduled_jobs.append({"name": spec["name"], "last_run": last_run_str,
                                    "result": result_str, "next_expected": None, "level": level})
    except Exception as exc:
        app.logger.warning("pulse scheduled_jobs error: %s", exc)
        scheduled_jobs = [{"name": "Error", "last_run": None, "result": str(exc)[:60], "next_expected": None, "level": "error"}]

    # 6. Runaway detector
    runaway = {"writes_per_hour": [], "level": "ok", "error_msg": None}
    try:
        tables_to_watch = [("Compliance Log", 50), ("Insurance Policies", 30), ("Incoming Extractions", 40)]
        any_over = False
        for tname, normal_band in tables_to_watch:
            count = _hs.get_write_rate(tname, window_minutes=60)
            if count > normal_band:
                any_over = True
            runaway["writes_per_hour"].append({"table": tname, "count": count, "normal_band": normal_band})
        if any_over:
            runaway["level"] = "warn"
    except Exception as exc:
        app.logger.warning("pulse runaway error: %s", exc)
        runaway["level"] = "error"
        runaway["error_msg"] = str(exc)[:120]

    # 7. Outside services
    services = []
    try:
        at_level = "ok" if airtable_space["total_records"] is not None else "error"
        services.append({"name": "Airtable API", "status": "Reachable" if at_level == "ok" else "Unreachable",
                          "last_ok": _now_iso() if at_level == "ok" else None, "level": at_level, "detail": ""})
        ai_key = (config.ANTHROPIC_API_KEY if hasattr(config, "ANTHROPIC_API_KEY") else os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        if ai_key and ai_key.startswith("sk-"):
            services.append({"name": "AI API (Anthropic)", "status": "Key present", "last_ok": None, "level": "ok", "detail": ""})
        else:
            services.append({"name": "AI API (Anthropic)", "status": "Key missing", "last_ok": None, "level": "error",
                              "detail": "ANTHROPIC_API_KEY not set or invalid"})
        sg_key = os.environ.get("SENDGRID_API_KEY", "").strip()
        services.append({"name": "SendGrid", "status": "Key present" if sg_key else "Key missing",
                          "last_ok": None, "level": "ok" if sg_key else "warn",
                          "detail": "" if sg_key else "SENDGRID_API_KEY not set"})
        email_ok = bool(os.environ.get("EMAIL_ADDRESS") and os.environ.get("EMAIL_PASSWORD"))
        services.append({"name": "Gmail inbox",
                          "status": "Credentials present" if email_ok else "Credentials missing",
                          "last_ok": None, "level": "ok" if email_ok else "error",
                          "detail": "" if email_ok else "EMAIL_ADDRESS or EMAIL_PASSWORD not set"})
    except Exception as exc:
        app.logger.warning("pulse services error: %s", exc)

    # 8. Waiting on Haley
    waiting_on_haley = {
        "queue_count": 0, "oldest_queue_days": None, "unsent_drafts": 0,
        "followups_due_today": 0, "skipped_emails": email_intake["today_skipped"], "no_contact": 0,
        "error_msg": None,
    }
    try:
        now_utc2 = datetime.now(tz=timezone.utc)
        ie_tbl2 = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)
        pending_recs = ie_tbl2.all(formula="{Review Status}='Pending Review'", fields=["Review Status"])
        waiting_on_haley["queue_count"] = len(pending_recs)
        oldest_days = None
        for rec in pending_recs:
            created_raw = (rec.get("createdTime") or "").strip()
            if created_raw:
                try:
                    dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                    age = (now_utc2 - dt).days
                    if oldest_days is None or age > oldest_days:
                        oldest_days = age
                except ValueError:
                    pass
        waiting_on_haley["oldest_queue_days"] = oldest_days
        try:
            em_tbl2 = api.table(base_id, EMAIL_MESSAGES_TABLE_ID)
            drafts = em_tbl2.all(formula="AND({Status}='Draft',NOT({Snoozed Until}))", fields=["Status"])
            waiting_on_haley["unsent_drafts"] = len(drafts)
        except Exception:
            pass
        try:
            no_contact = api.table(base_id, "Vendors").all(formula="NOT({Vendor Email})", fields=["Vendor Email"])
            waiting_on_haley["no_contact"] = len(no_contact)
        except Exception:
            pass
        try:
            em_replies = api.table(base_id, EMAIL_MESSAGES_TABLE_ID).all(
                formula="{Status}='Sent by Haley'", fields=["Status"])
            waiting_on_haley["replies_waiting"] = len(em_replies)
        except Exception:
            waiting_on_haley["replies_waiting"] = 0
        try:
            from marketing_constants import MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID, MI_STATUS, MI_STATUS_NEW
            mkt_tbl = _api().table(MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID)
            mkt_new = mkt_tbl.all(formula=f"{{{MI_STATUS}}}='{MI_STATUS_NEW}'", fields=[MI_STATUS])
            waiting_on_haley["marketing_new"] = len(mkt_new)
        except Exception:
            waiting_on_haley["marketing_new"] = 0
    except Exception as exc:
        app.logger.warning("pulse waiting_on_haley error: %s", exc)

    # 9. Business snapshot
    business = {"clients": 0, "vendors_tracked": 0, "certs_on_file": 0, "open_gaps": 0, "per_client": [], "error_msg": None}
    try:
        clients_recs = api.table(base_id, "Clients").all(fields=["Client Name"])
        business["clients"] = len(clients_recs)
        client_list = [{"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]} for r in clients_recs]
        business["vendors_tracked"] = len(api.table(base_id, "Vendors").all(fields=["Vendor Name"]))
        business["certs_on_file"] = len(api.table(base_id, "Insurance Certificates").all(fields=["Certificate Name"]))
        try:
            gap_recs = api.table(base_id, "Gap Requests").all(fields=["Request ID"])
            business["open_gaps"] = len(gap_recs)
        except Exception:
            pass
        try:
            reqs_all = api.table(base_id, "Client Requirements").all(fields=["Client Link"])
            reqs_by_client = {}
            for r in reqs_all:
                for cid in (r["fields"].get("Client Link") or []):
                    reqs_by_client[cid] = reqs_by_client.get(cid, 0) + 1
            vca_recs = api.table(base_id, "Vendor Client Assignments").all(fields=["Client Link"])
            certs_by_client = {}
            for r in vca_recs:
                for cid in (r["fields"].get("Client Link") or []):
                    certs_by_client[cid] = certs_by_client.get(cid, 0) + 1
            per_client = [{"name": c["name"], "certs_on_file": certs_by_client.get(c["id"], 0),
                            "certs_required": reqs_by_client.get(c["id"], 0)} for c in client_list]
            per_client.sort(key=lambda r: r["name"])
            business["per_client"] = per_client
        except Exception:
            pass
    except Exception as exc:
        app.logger.warning("pulse business error: %s", exc)
        business["error_msg"] = str(exc)[:120]

    # Recent events
    try:
        recent_events = _hs.get_recent_events(limit=50, since_hours=24)
    except Exception:
        recent_events = []

    # Schema check (cached 1hr)
    try:
        _api_key = (os.environ.get("AIRTABLE_API_KEY") or "").strip()
        schema_mismatches = _pulse_schema_mismatches(base_id, _api_key)
    except Exception:
        schema_mismatches = []

    # ── Audits section ────────────────────────────────────────────────────────
    try:
        from audit_store import _conn as _audit_conn
        with _audit_conn() as _aconn:
            _audit_rows = _aconn.execute(
                """SELECT a.id, a.client_name, a.coverage, a.start_date, a.end_date,
                          a.status, a.due_date,
                          COUNT(DISTINCT p.id) as payment_count,
                          COUNT(DISTINCT pc.id) as payee_count
                   FROM audits a
                   LEFT JOIN payments p ON p.audit_id = a.id AND p.excluded = 0
                   LEFT JOIN payee_classifications pc ON pc.audit_id = a.id
                   WHERE a.status NOT IN ('submitted', 'archived')
                   GROUP BY a.id
                   ORDER BY a.created_at DESC
                   LIMIT 10"""
            ).fetchall()
        active_audits = []
        today_iso = date.today().isoformat()
        for row in _audit_rows:
            r = dict(row)
            days_until_due = None
            if r.get("due_date"):
                try:
                    dd = date.fromisoformat(r["due_date"])
                    days_until_due = (dd - date.today()).days
                except Exception:
                    pass
            active_audits.append({
                "id":           r["id"],
                "client_name":  r["client_name"],
                "coverage":     r["coverage"],
                "period":       f"{r['start_date']} – {r['end_date']}",
                "status":       r["status"],
                "payment_count": r.get("payment_count", 0),
                "payee_count":  r.get("payee_count", 0),
                "due_date":     r.get("due_date", ""),
                "days_until_due": days_until_due,
            })
        audits_section = {
            "active_audits": active_audits,
            "in_progress": len(active_audits),
            "nearest_due_days": min(
                (a["days_until_due"] for a in active_audits if a["days_until_due"] is not None),
                default=None
            ),
            "error_msg": None,
        }
    except Exception as _exc:
        audits_section = {
            "active_audits": [], "in_progress": 0,
            "nearest_due_days": None, "error_msg": str(_exc)[:120],
        }

    pulse_data = {
        "airtable_space": airtable_space, "email_intake": email_intake,
        "pipeline": pipeline, "writes_errors": writes_errors,
        "scheduled_jobs": scheduled_jobs, "runaway": runaway,
        "services": services, "waiting_on_haley": waiting_on_haley,
        "business": business, "recent_events": recent_events,
        "schema_mismatches": schema_mismatches,
        "audits": audits_section,
    }

    section_levels = (
        [airtable_space["level"], email_intake["level"], pipeline["level"],
         writes_errors["level"], runaway["level"]]
        + [s["level"] for s in services]
        + [j["level"] for j in scheduled_jobs]
    )
    overall_level = _level_max(*section_levels) if section_levels else "ok"

    _pulse_settings_defaults = {
        "airtable_warn_pct": 60, "airtable_error_pct": 80,
        "cb_max_writes": 200, "cb_window_minutes": 10,
        "oldest_pending_warn_days": 3, "oldest_pending_error_days": 7,
        "skipped_emails_warn": 1, "skipped_emails_error": 5,
        "summary_email_enabled": False, "summary_email_hour_utc": 14,
        "quiet_hours_start_utc": 2, "quiet_hours_end_utc": 11,
    }
    try:
        _sp = os.path.join(os.path.dirname(__file__), "pulse_settings.json")
        with open(_sp) as _f:
            pulse_settings_data = {**_pulse_settings_defaults, **json.load(_f)}
    except Exception:
        pulse_settings_data = _pulse_settings_defaults

    return render_template("pulse.html", pulse_data=pulse_data,
                            overall_level=overall_level, last_check=_now_iso(),
                            pulse_settings=pulse_settings_data)


@app.route("/pulse/settings", methods=["POST"])
@login_required
def pulse_settings():
    """Save Pulse threshold settings to pulse_settings.json."""
    from flask import jsonify
    try:
        data = request.get_json(force=True) or {}
        settings_path = os.path.join(os.path.dirname(__file__), "pulse_settings.json")
        existing = {}
        if os.path.exists(settings_path):
            try:
                with open(settings_path) as fh:
                    existing = json.load(fh)
            except Exception:
                existing = {}
        existing.update(data)
        with open(settings_path, "w") as fh:
            json.dump(existing, fh, indent=2)
        return jsonify({"ok": True})
    except Exception as exc:
        app.logger.error("pulse_settings error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/pulse/mark-not-needed", methods=["POST"])
@login_required
def pulse_mark_not_needed():
    """Mark an Incoming Document as 'Not needed'."""
    from flask import jsonify
    try:
        data = request.get_json(force=True) or {}
        doc_id = (data.get("doc_id") or "").strip()
        if not doc_id:
            return jsonify({"ok": False, "error": "doc_id required"}), 400
        base_id = clean_base_id(config.AIRTABLE_BASE_ID)
        _api().table(base_id, "Incoming Documents").update(doc_id, {"Status": "Not needed"}, typecast=True)
        _hs.record_event("pipeline", "ok", f"Marked doc {doc_id} as not needed", table_name="Incoming Documents")
        return jsonify({"ok": True})
    except Exception as exc:
        app.logger.error("pulse_mark_not_needed error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/pulse/reprocess", methods=["POST"])
@login_required
def pulse_reprocess():
    """Queue an Incoming Document for reprocessing."""
    from flask import jsonify
    try:
        data = request.get_json(force=True) or {}
        doc_id = (data.get("doc_id") or "").strip()
        if not doc_id:
            return jsonify({"ok": False, "error": "doc_id required"}), 400
        base_id = clean_base_id(config.AIRTABLE_BASE_ID)
        doc_tbl = _api().table(base_id, "Incoming Documents")
        rec = doc_tbl.get(doc_id)
        if not rec:
            return jsonify({"ok": False, "error": "Record not found"}), 404
        file_names = (rec["fields"].get("File Names") or "").strip()
        doc_tbl.update(doc_id, {"Status": "New"}, typecast=True)
        if file_names:
            target_file = file_names.split(",")[0].strip()
            py_bin = os.path.join(os.path.dirname(__file__), ".venv", "bin", "python")
            pipeline_script = os.path.join(os.path.dirname(__file__), "run_pipeline.py")
            env = dict(os.environ, COI_TARGET_FILE=target_file)
            def _run():
                try:
                    subprocess.run([py_bin, pipeline_script], env=env,
                                   cwd=os.path.dirname(__file__), timeout=300)
                except Exception as sub_exc:
                    app.logger.warning("pulse_reprocess subprocess error: %s", sub_exc)
            threading.Thread(target=_run, daemon=True).start()
        _hs.record_event("pipeline", "ok", f"Reprocess triggered for doc {doc_id}",
                          body=file_names or None, table_name="Incoming Documents")
        return jsonify({"ok": True, "queued": True})
    except Exception as exc:
        app.logger.error("pulse_reprocess error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/pulse/test-email", methods=["POST"])
@login_required
def pulse_test_email():
    """Send a test Pulse summary email to haley@."""
    from flask import jsonify
    try:
        sg_key = (os.environ.get("SENDGRID_API_KEY") or "").strip()
        if not sg_key:
            return jsonify({"ok": False, "error": "SENDGRID_API_KEY not set"}), 500
        from_email_addr = (os.environ.get("SENDGRID_FROM_EMAIL") or config.FROM_EMAIL).strip()
        to_email = config.OWNER_EMAIL
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, Email
        message = Mail(
            from_email=Email(from_email_addr, "CCS Pulse"),
            to_emails=to_email,
            subject="[CCS Pulse] Test alert — system is healthy",
            plain_text_content=(
                "This is a test message from the CCS Pulse health dashboard.\n\n"
                "If you received this, your Pulse email notifications are working.\n\n"
                "— Carolina Compliance Solutions"
            ),
        )
        sg = SendGridAPIClient(api_key=sg_key)
        sg.send(message)
        _hs.record_event("heartbeat", "ok", "test_email_sent", body=f"sent to {to_email}")
        return jsonify({"ok": True, "sent_to": to_email})
    except Exception as exc:
        app.logger.error("pulse_test_email error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Audit Workspace ──────────────────────────────────────────────────────────
#
# Payments, payee classifications, and audit files are stored in SQLite
# (audit_store.py) — NOT in Airtable.  Airtable receives only one lightweight
# record per audit.

import audit_store as _astore
import audit_parser as _aparser

_AUDIT_UPLOADS_DIR = os.path.join(os.path.dirname(__file__), "audit_uploads")
os.makedirs(_AUDIT_UPLOADS_DIR, exist_ok=True)

# Keywords that indicate a payee is likely NOT a subcontractor
_NOT_SUB_SIGNALS = frozenset({
    "bank", "loan", "mortgage", "leasing", "capital one", "credit union",
    "duke energy", "piedmont natural gas", "spectrum", "atandt", "at&t",
    "adp", "paychex", "gusto", "payroll",
    "erie", "nationwide", "travelers", "insurance", "allstate", "liberty mutual",
    "quickbooks", "intuit", "adobe", "microsoft", "google",
    "shell", "bp", "circle k", "speedway", "exxon", "chevron", "speedco",
    "amazon", "home depot", "lowe's", "lowes", "menards", "costco", "walmart",
    "irs", "ncdor", "tax", "dor",
    "transfer", "zelle", "venmo", "paypal",
    "credit card payment", "card payment", "loan payment",
    "fuel", "gas station",
})

# Trade keywords that suggest a payee is a subcontractor
_SUB_TRADE_SIGNALS = frozenset({
    "construction", "remodeling", "renovations", "renovating",
    "electric", "electrical", "plumbing", "hvac", "mechanical",
    "drywall", "roofing", "framing", "grading", "excavating", "masonry",
    "tile", "flooring", "painting", "insulation", "siding",
    "windows", "doors", "concrete", "foundation", "landscaping",
    "excavation", "cleanup", "cleanout", "demo", "demolition",
    "carpentry", "general", "contracting", "contractor",
    "installation", "installing",
})


def _norm_payee(name: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", (name or "").lower())).strip()


def _payee_looks_like_not_sub(name: str) -> bool:
    nl = (name or "").lower()
    return any(sig in nl for sig in _NOT_SUB_SIGNALS)


def _payee_looks_like_sub(name: str) -> bool:
    nl = (name or "").lower()
    return any(sig in nl for sig in _SUB_TRADE_SIGNALS)


def _fuzzy_match_vendors(
    payee_raw: str,
    vendor_list: list[dict],
    client_id: str,
) -> tuple[str, str, str, str]:
    """Return (vendor_id, vendor_name, match_type, payee_group).

    match_type: 'exact' | 'likely' | 'possible' | ''
    payee_group: 'ccs_vendor' | 'likely_sub' | 'unclear' | 'not_sub'
    """
    from rapidfuzz import fuzz as _fuzz

    # 1. Check saved aliases first
    alias = _astore.get_alias(client_id, payee_raw)
    if alias:
        return alias["vendor_id"], alias["vendor_name"], "exact", "ccs_vendor"

    pnorm = _norm_payee(payee_raw)

    best_id    = ""
    best_name  = ""
    best_score = 0

    for v in vendor_list:
        vnorm = _norm_payee(v["name"])
        if not vnorm:
            continue
        score = _fuzz.token_set_ratio(pnorm, vnorm)
        if score > best_score:
            best_score = score
            best_id    = v["id"]
            best_name  = v["name"]

    if best_score >= 90:
        return best_id, best_name, "exact", "ccs_vendor"
    if best_score >= 70:
        return best_id, best_name, "likely", "ccs_vendor"
    if best_score >= 50:
        return best_id, best_name, "possible", "unclear"

    # No match — classify by payee name signals
    if _payee_looks_like_not_sub(payee_raw):
        return "", "", "", "not_sub"
    if _payee_looks_like_sub(payee_raw):
        return "", "", "", "likely_sub"
    return "", "", "", "unclear"


def _classify_all_payees(audit_id: str, client_id: str, vendor_list: list[dict]) -> None:
    """Run fuzzy match on all unique payees and upsert classifications."""
    with _astore._conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT payee_raw FROM payments WHERE audit_id = ? AND excluded = 0",
            (audit_id,),
        ).fetchall()
    payees = [r[0] for r in rows]

    for payee_raw in payees:
        existing = _astore.get_payee_classification(audit_id, payee_raw)
        if existing and existing.get("confirmed"):
            continue  # never overwrite confirmed decisions

        vendor_id, vendor_name, match_type, payee_group = _fuzzy_match_vendors(
            payee_raw, vendor_list, client_id,
        )
        _astore.upsert_payee_classification(
            audit_id=audit_id,
            client_id=client_id,
            payee_raw=payee_raw,
            vendor_id=vendor_id,
            vendor_name=vendor_name,
            match_type=match_type,
            payee_group=payee_group,
            confirmed=0,
        )


def _get_vendor_list_for_client(client_id: str) -> list[dict]:
    """Return [{id, name}] for all vendors linked to this client, plus all vendors."""
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    try:
        recs = api.table(base_id, VENDORS_TABLE_ID).all(fields=["Vendor Name"])
        return [
            {"id": r["id"], "name": r["fields"].get("Vendor Name") or ""}
            for r in recs if r["fields"].get("Vendor Name")
        ]
    except Exception:
        return []


def _compute_audit_analysis(audit: dict) -> dict:
    """Full analysis for an audit: payments + coverage per payee.

    Returns:
        {
          "subs": [{payee_raw, vendor_id, vendor_name, match_type, payee_group,
                    txn_count, total_paid, first_date, last_date,
                    terms, status, days_not_on_file, gap_segments,
                    paid_in_gaps, wc_on_file (GL audits only),
                    cert_received_date, remediation}],
          "headline": {subs_count, total_paid, full_count, full_dollars,
                       partial_count, partial_dollars, missing_count, missing_dollars,
                       not_in_ccs_count, not_in_ccs_dollars, paid_in_gaps_total},
        }
    """
    audit_id    = audit["id"]
    client_id   = audit["client_id"]
    coverage    = audit["coverage"]
    start_date  = _parse_date(audit["start_date"])
    end_date    = _parse_date(audit["end_date"])

    if not start_date or not end_date:
        return {"subs": [], "headline": {}}

    # Get payee summaries within audit period
    pay_summary = _astore.get_payee_summary(
        audit_id, audit["start_date"], audit["end_date"]
    )
    pay_by_payee = {p["payee_raw"]: p for p in pay_summary}

    # Get classifications
    classifications = {
        c["payee_raw"]: c
        for c in _astore.get_payee_classifications(audit_id)
    }

    # Get remediation notes
    remediation = _astore.get_remediation(audit_id)

    # GL audit: also pull WC terms per vendor
    is_gl = "general" in coverage.lower() or coverage.upper() == "GL"

    subs = []
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    # ── Batch prefetch all vendor → policies in 2 Airtable calls ──────────────
    # Collect all vendor_ids for ccs_vendor payees
    ccs_vendor_ids = [
        classifications[pr]["vendor_id"]
        for pr in pay_by_payee
        if classifications.get(pr, {}).get("payee_group") == "ccs_vendor"
        and classifications.get(pr, {}).get("vendor_id")
    ]
    # vendor_id → list of policy records
    _vendor_policies_cache: dict[str, list] = {}
    if ccs_vendor_ids:
        try:
            # 1 call: fetch all vendor records to get linked policy IDs
            vend_formula = "OR(" + ", ".join(f"RECORD_ID()='{v}'" for v in ccs_vendor_ids) + ")"
            vend_records = api.table(base_id, VENDORS_TABLE_ID).all(
                formula=vend_formula,
                fields=["Insurance Policies"],
            )
            all_policy_ids: list[str] = []
            vendor_to_policy_ids: dict[str, list[str]] = {}
            for vrec in vend_records:
                pol_ids = vrec["fields"].get("Insurance Policies") or []
                vendor_to_policy_ids[vrec["id"]] = pol_ids
                all_policy_ids.extend(pol_ids)
            # 1 call: fetch all policy records
            if all_policy_ids:
                pol_formula = "OR(" + ", ".join(f"RECORD_ID()='{p}'" for p in all_policy_ids) + ")"
                pol_records = api.table(base_id, INSURANCE_POLICIES_TABLE_ID).all(
                    formula=pol_formula,
                    fields=["Policy Type", "Policy Number", "Carrier",
                            "Effective Date", "Expiration Date",
                            "Last Updated From Extraction"],
                )
                pol_by_id = {r["id"]: r for r in pol_records}
                for vid, pol_ids in vendor_to_policy_ids.items():
                    _vendor_policies_cache[vid] = [
                        pol_by_id[pid] for pid in pol_ids if pid in pol_by_id
                    ]
        except Exception as exc:
            app.logger.warning("audit batch policy prefetch failed: %s", exc)

    def _terms_from_cache(vendor_id: str, audit_type: str) -> tuple[list, bool]:
        """Like _terms_for_vendor_audit but uses prefetched policy cache."""
        kws = _audit_coverage_keywords(audit_type)
        pols = _vendor_policies_cache.get(vendor_id, [])
        if vendor_id not in _vendor_policies_cache:
            # Fallback to live call if cache miss
            return _terms_for_vendor_audit(vendor_id, audit_type, start_date, end_date)
        terms = []
        has_any = False
        for rec in pols:
            pf = rec["fields"]
            pt = (pf.get("Policy Type") or "").lower()
            if not _pol_matches_audit_type(pt, kws):
                continue
            has_any = True
            eff = _parse_date(pf.get("Effective Date"))
            exp = _parse_date(pf.get("Expiration Date"))
            if not eff or not exp:
                continue
            if eff <= end_date and exp >= start_date:
                terms.append({
                    "record_id":      rec["id"],
                    "policy_number":  pf.get("Policy Number") or "",
                    "carrier":        pf.get("Carrier") or "",
                    "effective":      eff.isoformat(),
                    "expiration":     exp.isoformat(),
                    "extraction_ids": pf.get("Last Updated From Extraction") or [],
                })
        terms.sort(key=lambda t: t["effective"])
        return terms, has_any

    def _cert_received_from_cache(vendor_id: str, audit_type: str) -> str:
        kws = _audit_coverage_keywords(audit_type)
        pols = _vendor_policies_cache.get(vendor_id, [])
        best = ""
        for rec in pols:
            pf = rec["fields"]
            pt = (pf.get("Policy Type") or "").lower()
            if not _pol_matches_audit_type(pt, kws):
                continue
            ra = pf.get("Received At") or pf.get("Last Updated From Extraction") or ""
            if isinstance(ra, list):
                ra = ra[0] if ra else ""
            if ra and (not best or ra > best):
                best = ra[:10]
        return best
    # ── End batch prefetch ─────────────────────────────────────────────────────

    for payee_raw, ps in pay_by_payee.items():
        cls = classifications.get(payee_raw, {})
        pg  = cls.get("payee_group", "unclear")

        # Skip confirmed not-subs; skip payees with no meaningful amount
        if pg == "not_sub":
            continue

        vendor_id   = cls.get("vendor_id", "")
        vendor_name = cls.get("vendor_name", "") or payee_raw
        match_type  = cls.get("match_type", "")

        # Fetch coverage terms from Airtable
        terms_data: list[dict] = []
        days_not_on_file = (end_date - start_date).days + 1
        gap_segs: list[dict] = []
        status = "not_in_ccs"
        cert_received = ""
        wc_on_file = False

        # Only look up Airtable data for confirmed CCS vendors; unclear payees
        # show as "Not in CCS" without burning API calls for unconfirmed matches.
        if vendor_id and pg == "ccs_vendor":
            try:
                terms_raw, has_any = _terms_from_cache(vendor_id, coverage)
                status_str, days_not_on_file, gap_runs = _compute_vendor_audit_status(
                    terms_raw, start_date, end_date, has_any
                )
                status = status_str

                for t in terms_raw:
                    terms_data.append({
                        "policy_number": t["policy_number"],
                        "carrier":       t["carrier"],
                        "effective":     t["effective"],
                        "expiration":    t["expiration"],
                    })

                for gs, ge in gap_runs:
                    gap_segs.append({
                        "start": gs.isoformat(),
                        "end":   ge.isoformat(),
                        "days":  (ge - gs).days + 1,
                    })

                cert_received = _cert_received_from_cache(vendor_id, coverage)

                # WC on file (informational, for GL audits)
                if is_gl:
                    try:
                        wc_terms, wc_has = _terms_from_cache(vendor_id, "Workers' Comp")
                        wc_on_file = bool(wc_terms) or wc_has
                    except Exception:
                        wc_on_file = False

            except Exception:
                status = "not_in_ccs"

        # Compute "paid in gaps" — payments that fall in a gap period
        paid_in_gaps = 0.0
        if gap_segs and vendor_id:
            payments_detail = _astore.get_payments(
                audit_id, payee_raw=payee_raw,
                in_period_only=True,
                start_date=audit["start_date"], end_date=audit["end_date"],
            )
            gap_date_ranges = [
                (_parse_date(g["start"]), _parse_date(g["end"]))
                for g in gap_segs
            ]
            for pmt in payments_detail:
                pd = _parse_date(pmt["txn_date"])
                if pd and any(gs <= pd <= ge for gs, ge in gap_date_ranges if gs and ge):
                    paid_in_gaps += pmt["amount"]
        elif status in ("no_policy", "none_in_window", "not_in_ccs"):
            # Entire period is a gap
            paid_in_gaps = float(ps["total_amount"] or 0)

        rem = remediation.get(payee_raw, {})

        # Human-readable status label
        status_labels = {
            "full":          "On file for the whole period",
            "short_gap":     "On file for part of the period",
            "gap":           "On file for part of the period",
            "none_in_window": "Not on file",
            "no_policy":     "Not on file",
            "not_in_ccs":    "Not in CCS yet",
        }

        subs.append({
            "payee_raw":      payee_raw,
            "vendor_id":      vendor_id,
            "vendor_name":    vendor_name,
            "match_type":     match_type,
            "payee_group":    pg,
            "trade":          cls.get("trade", ""),
            "confirmed":      bool(cls.get("confirmed")),
            "txn_count":      ps["txn_count"],
            "total_paid":     round(float(ps["total_amount"] or 0), 2),
            "first_date":     ps["first_date"],
            "last_date":      ps["last_date"],
            "terms":          terms_data,
            "status":         status,
            "status_label":   status_labels.get(status, status),
            "days_not_on_file": days_not_on_file,
            "gap_segments":   gap_segs,
            "paid_in_gaps":   round(paid_in_gaps, 2),
            "wc_on_file":     wc_on_file if is_gl else None,
            "cert_received":  cert_received,
            "remediation":    rem.get("status", ""),
            "remediation_note": rem.get("note", ""),
        })

    # Sort by paid_in_gaps descending (biggest risk first)
    subs.sort(key=lambda s: (-s["paid_in_gaps"], -s["total_paid"]))

    # Headline
    total_paid    = sum(s["total_paid"] for s in subs)
    full_subs     = [s for s in subs if s["status"] == "full"]
    partial_subs  = [s for s in subs if s["status"] in ("short_gap", "gap")]
    missing_subs  = [s for s in subs if s["status"] in ("no_policy", "none_in_window")]
    not_ccs_subs  = [s for s in subs if s["status"] == "not_in_ccs"]
    paid_in_gaps_total = sum(s["paid_in_gaps"] for s in subs)

    headline = {
        "subs_count":       len(subs),
        "total_paid":       round(total_paid, 2),
        "full_count":       len(full_subs),
        "full_dollars":     round(sum(s["total_paid"] for s in full_subs), 2),
        "partial_count":    len(partial_subs),
        "partial_dollars":  round(sum(s["total_paid"] for s in partial_subs), 2),
        "missing_count":    len(missing_subs),
        "missing_dollars":  round(sum(s["total_paid"] for s in missing_subs), 2),
        "not_in_ccs_count": len(not_ccs_subs),
        "not_in_ccs_dollars": round(sum(s["total_paid"] for s in not_ccs_subs), 2),
        "paid_in_gaps_total": round(paid_in_gaps_total, 2),
    }

    return {"subs": subs, "headline": headline}


def _build_audit_cert_request_body(
    contact: dict,
    client_name: str,
    cert_holder: str,
    subs: list,
    audit_start: str,
    audit_end: str,
    coverage: str,
) -> str:
    """Build a batched certificate request email body for an audit."""
    greeting   = contact.get("name") or "Insurance Agent"
    agency_str = f" at {contact['agency']}" if contact.get("agency") else ""

    def _fd(s):
        d = _parse_date(s)
        return d.strftime("%m/%d/%Y") if d else s

    lines = [
        f"Hello {greeting}{agency_str},",
        "",
        f"I am writing on behalf of {client_name} to request certificates of insurance "
        f"for a {coverage} audit covering the period {_fd(audit_start)} to {_fd(audit_end)}.",
        "",
        "Please reply with updated certificates for the following subcontractors:",
        "",
    ]
    for sub in subs:
        lines.append(f"  {sub['vendor_name']}")
        gaps = sub.get("gap_segments") or []
        if gaps:
            for g in gaps:
                lines.append(
                    f"    Period not on file: {_fd(g['start'])} to {_fd(g['end'])} "
                    f"({g['days']} days)"
                )
        else:
            lines.append(f"    No certificate on file for the audit period")
        lines.append("")

    lines += [
        "Please issue certificates with the following certificate holder:",
        f"  {cert_holder}",
        "",
        "Certificates should be sent to coi-intake@carolinacompliancesolutions.com "
        "and will be processed automatically.",
        "",
        "Thank you,",
        "Carolina Compliance Solutions",
        f"On behalf of {client_name}",
    ]
    return "\n".join(lines)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/audits")
@login_required
def audits_list():
    """List all audits (new Audit Workspace entry point)."""
    from flask import jsonify
    client_filter = request.args.get("client_id", "").strip()

    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    clients_raw = api.table(base_id, CLIENTS_TABLE_ID).all(fields=["Client Name"])
    clients = sorted(
        [{"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]} for r in clients_raw],
        key=lambda c: c["name"],
    )
    client_map = {c["id"]: c["name"] for c in clients}

    all_audits = _astore.list_audits(client_id=client_filter or None)
    # Enrich with client name from live map (in case it changed)
    for a in all_audits:
        if a["client_id"] in client_map:
            a["client_name"] = client_map[a["client_id"]]

    return render_template(
        "audits.html",
        audits=all_audits,
        clients=clients,
        client_filter=client_filter,
        rtr_client_id=RTR_CLIENT_ID,
    )


@app.route("/audits/new", methods=["GET", "POST"])
@login_required
@csrf_protect
def audits_new():
    """Create a new audit and redirect to workspace."""
    from flask import jsonify
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    if request.method == "GET":
        clients_raw = api.table(base_id, CLIENTS_TABLE_ID).all(fields=["Client Name"])
        clients = sorted(
            [{"id": r["id"], "name": r["fields"].get("Client Name") or r["id"]} for r in clients_raw],
            key=lambda c: c["name"],
        )
        return render_template("audits.html", audits=[], clients=clients,
                               client_filter="", rtr_client_id=RTR_CLIENT_ID, show_new_form=True)

    # POST
    client_id    = (request.form.get("client_id") or RTR_CLIENT_ID).strip()
    coverage     = (request.form.get("coverage") or "General Liability").strip()
    start_date   = (request.form.get("start_date") or "").strip()
    end_date     = (request.form.get("end_date") or "").strip()
    auditor_name = (request.form.get("auditor_name") or "").strip()
    policy_num   = (request.form.get("policy_number") or "").strip()
    notes        = (request.form.get("notes") or "").strip()
    due_date     = (request.form.get("due_date") or "").strip()

    if not start_date or not end_date:
        flash("Start date and end date are required.", "danger")
        return redirect(url_for("audits_list"))

    try:
        crec = api.table(base_id, CLIENTS_TABLE_ID).get(client_id)
        client_name = crec["fields"].get("Client Name") or client_id
    except Exception:
        client_name = client_id

    audit = _astore.create_audit(
        client_id=client_id,
        client_name=client_name,
        coverage=coverage,
        start_date=start_date,
        end_date=end_date,
        auditor_name=auditor_name,
        policy_number=policy_num,
        notes=notes,
        due_date=due_date,
    )

    # Create lightweight Airtable record
    try:
        abbrev = "GL" if "general" in coverage.lower() else "WC"
        audit_name = f"{client_name} {abbrev} {start_date[:7]} to {end_date[:7]}"
        at_rec = api.table(base_id, AUDITS_TABLE_ID).create({
            "Audit Name":  audit_name,
            "Client":      [client_id],
            "Audit Type":  coverage,
            "Start Date":  start_date,
            "End Date":    end_date,
            "Created At":  datetime.now(timezone.utc).isoformat(),
        })
        _astore.update_audit(audit["id"], airtable_id=at_rec["id"])
    except Exception as exc:
        app.logger.warning("audits_new: Airtable record creation failed: %s", exc)

    return redirect(url_for("audit_workspace", audit_id=audit["id"]))


@app.route("/audits/<audit_id>")
@login_required
def audit_workspace(audit_id):
    """Main audit workspace page."""
    audit = _astore.get_audit(audit_id)
    if not audit:
        abort(404)

    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    # Client info
    try:
        crec = api.table(base_id, CLIENTS_TABLE_ID).get(audit["client_id"])
        cf = crec["fields"]
        client_name   = cf.get("Client Name") or audit["client_name"]
        cert_holder   = cf.get("Certificate Holder") or client_name
    except Exception:
        client_name = audit["client_name"]
        cert_holder = client_name

    files = _astore.get_audit_files(audit_id)
    packs = _astore.get_audit_packs(audit_id)

    # Count pending decisions for checklist badge
    cls_all = _astore.get_payee_classifications(audit_id)
    unconfirmed_count = sum(
        1 for c in cls_all
        if not c.get("confirmed") and c.get("payee_group") not in ("not_sub",)
    )
    possible_count = sum(1 for c in cls_all if c.get("match_type") == "possible")
    unclear_count  = sum(1 for c in cls_all if c.get("payee_group") == "unclear")

    return render_template(
        "audit_workspace.html",
        audit=audit,
        client_name=client_name,
        cert_holder=cert_holder,
        files=files,
        packs=packs,
        unconfirmed_count=unconfirmed_count,
        possible_count=possible_count,
        unclear_count=unclear_count,
        rtr_client_id=RTR_CLIENT_ID,
    )


@app.route("/audits/<audit_id>/upload", methods=["POST"])
@login_required
@csrf_protect
def audit_upload(audit_id):
    """Upload and parse a payment file. Returns JSON."""
    from flask import jsonify
    audit = _astore.get_audit(audit_id)
    if not audit:
        return jsonify({"ok": False, "error": "Audit not found"}), 404

    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"ok": False, "error": "No filename"}), 400

    file_bytes = f.read()
    original_filename = f.filename

    # Save file to disk
    safe_name = re.sub(r"[^a-zA-Z0-9._\-]", "_", original_filename)
    import uuid as _uuid_mod
    file_id_prefix = str(_uuid_mod.uuid4())[:8]
    stored_name = f"{file_id_prefix}_{safe_name}"
    file_path = os.path.join(_AUDIT_UPLOADS_DIR, stored_name)
    with open(file_path, "wb") as fh:
        fh.write(file_bytes)

    # Parse
    result = _aparser.parse_payment_file(original_filename, file_bytes)
    warnings = result.get("warnings", [])

    # Check date coverage
    cov_warnings = _aparser.check_file_covers_period(
        result.get("date_min", ""),
        result.get("date_max", ""),
        audit["start_date"],
        audit["end_date"],
    )
    warnings = cov_warnings + warnings

    file_id = _astore.create_audit_file(
        audit_id=audit_id,
        original_filename=original_filename,
        file_path=file_path,
        file_format=result["format"],
    )
    _astore.update_audit_file(
        file_id,
        file_format=result["format"],
        row_count=result["row_count"],
        total_amount=result["total_amount"],
        date_min=result["date_min"],
        date_max=result["date_max"],
        parse_status="ok" if result["row_count"] > 0 else "error",
        parse_notes="\n".join(warnings),
    )

    if result["row_count"] > 0:
        # Insert payments into SQLite
        pay_rows = []
        for t in result["transactions"]:
            pay_rows.append({
                "audit_id":       audit_id,
                "file_id":        file_id,
                "txn_date":       t["txn_date"],
                "txn_id":         t.get("txn_id", ""),
                "txn_num":        t.get("txn_num", ""),
                "txn_type":       t.get("txn_type", ""),
                "account":        t.get("account", ""),
                "payee_raw":      t["payee_raw"],
                "amount":         t["amount"],
                "is_card":        t.get("is_card", 0),
                "payment_method": t.get("payment_method", ""),
                "excluded":       0,
                "exclude_reason": "",
            })
        inserted = _astore.insert_payments(pay_rows)

        # Auto-classify payees
        vendor_list = _get_vendor_list_for_client(audit["client_id"])
        _classify_all_payees(audit_id, audit["client_id"], vendor_list)

        # Update audit status
        _astore.update_audit(audit_id, status="Gathering certificates")

    # Format summary for display
    fmt_summary = _format_file_summary(result, original_filename)

    # Preview of first few rows
    preview_rows = result["transactions"][:8] if result["transactions"] else []

    return jsonify({
        "ok":          True,
        "file_id":     file_id,
        "format":      result["format"],
        "summary":     fmt_summary,
        "row_count":   result["row_count"],
        "total_amount": result["total_amount"],
        "date_min":    result["date_min"],
        "date_max":    result["date_max"],
        "warnings":    warnings,
        "preview_rows": preview_rows[:8],
        "account_counts": result.get("account_counts", {}),
    })


def _format_file_summary(result: dict, filename: str) -> str:
    fmt_map = {
        "qb_transaction_list": "QuickBooks Transaction List",
        "qb_expense_summary_pdf": "QuickBooks Expenses by Vendor Summary (PDF)",
        "generic_csv": "CSV (generic)",
        "generic_xlsx": "Excel spreadsheet",
    }
    fmt_label = fmt_map.get(result["format"], result["format"])
    count = result["row_count"]
    date_min = result["date_min"]
    date_max = result["date_max"]

    def _fd(s):
        d = _parse_date(s)
        return d.strftime("%-m/%-d/%Y") if d else s

    return (
        f"I read this as: {fmt_label}, {count:,} payments, "
        f"{_fd(date_min)} to {_fd(date_max)}"
    )


@app.route("/audits/<audit_id>/payees")
@login_required
def audit_payees(audit_id):
    """Return payee classifications as JSON."""
    from flask import jsonify
    audit = _astore.get_audit(audit_id)
    if not audit:
        return jsonify({"ok": False, "error": "Not found"}), 404

    classifications = _astore.get_payee_classifications(audit_id)
    pay_summary = _astore.get_payee_summary(
        audit_id, audit["start_date"], audit["end_date"]
    )
    pay_by_payee = {p["payee_raw"]: p for p in pay_summary}

    result = []
    for cls in classifications:
        ps = pay_by_payee.get(cls["payee_raw"], {})
        result.append({
            **cls,
            "txn_count":   ps.get("txn_count", 0),
            "total_amount": round(float(ps.get("total_amount") or 0), 2),
            "first_date":  ps.get("first_date", ""),
            "last_date":   ps.get("last_date", ""),
        })

    result.sort(key=lambda r: (-float(r.get("total_amount") or 0), r["payee_raw"].lower()))

    # Group counts for display
    groups = {
        "ccs_vendor": 0, "likely_sub": 0, "unclear": 0, "not_sub": 0,
        "needs_confirm": 0,
    }
    for r in result:
        g = r.get("payee_group", "unclear")
        groups[g] = groups.get(g, 0) + 1
        if not r.get("confirmed") and r.get("match_type") in ("likely", "possible"):
            groups["needs_confirm"] += 1

    return jsonify({"ok": True, "payees": result, "groups": groups})


@app.route("/audits/<audit_id>/payees/update-one", methods=["POST"])
@login_required
@csrf_protect
def audit_payees_update_one(audit_id):
    """Update one payee classification."""
    from flask import jsonify
    audit = _astore.get_audit(audit_id)
    if not audit:
        return jsonify({"ok": False, "error": "Not found"}), 404

    data = request.get_json(force=True) or {}
    payee_raw  = (data.get("payee_raw") or "").strip()
    if not payee_raw:
        return jsonify({"ok": False, "error": "payee_raw required"}), 400

    # Fields that can be updated
    allowed = {
        "vendor_id", "vendor_name", "match_type", "payee_group",
        "trade", "is_supplier_only", "exclude_reason", "confirmed",
    }
    kwargs = {k: v for k, v in data.items() if k in allowed}
    kwargs["confirmed"] = 1  # any explicit update from Haley = confirmed

    _astore.upsert_payee_classification(
        audit_id=audit_id,
        client_id=audit["client_id"],
        payee_raw=payee_raw,
        **kwargs,
    )

    # If this is a vendor match, save as a confirmed alias for future uploads
    vendor_id   = kwargs.get("vendor_id", "")
    vendor_name = kwargs.get("vendor_name", "")
    if vendor_id and vendor_name and kwargs.get("payee_group") == "ccs_vendor":
        _astore.upsert_alias(
            client_id=audit["client_id"],
            payee_raw=payee_raw,
            vendor_id=vendor_id,
            vendor_name=vendor_name,
            confirmed=1,
        )

    return jsonify({"ok": True})


@app.route("/audits/<audit_id>/analysis")
@login_required
def audit_analysis(audit_id):
    """Return full coverage analysis as JSON."""
    from flask import jsonify
    audit = _astore.get_audit(audit_id)
    if not audit:
        return jsonify({"ok": False, "error": "Not found"}), 404

    try:
        result = _compute_audit_analysis(audit)
    except Exception as exc:
        app.logger.error("audit_analysis error for %s: %s", audit_id, exc)
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": True, **result})


@app.route("/audits/<audit_id>/payments")
@login_required
def audit_payments_detail(audit_id):
    """Return individual payments for a payee. Query: ?payee=<name>"""
    from flask import jsonify
    audit = _astore.get_audit(audit_id)
    if not audit:
        return jsonify({"ok": False, "error": "Not found"}), 404

    payee = request.args.get("payee", "").strip()
    rows = _astore.get_payments(
        audit_id, payee_raw=payee or None,
        in_period_only=True,
        start_date=audit["start_date"], end_date=audit["end_date"],
    )
    return jsonify({"ok": True, "payments": [dict(r) for r in rows]})


@app.route("/audits/<audit_id>/create-drafts", methods=["POST"])
@login_required
@csrf_protect
def audit_create_drafts(audit_id):
    """Create batched email drafts for an audit. Returns count of drafts created."""
    from flask import jsonify
    audit = _astore.get_audit(audit_id)
    if not audit:
        return jsonify({"ok": False, "error": "Not found"}), 404

    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        crec = api.table(base_id, CLIENTS_TABLE_ID).get(audit["client_id"])
        cf = crec["fields"]
        client_name = cf.get("Client Name") or audit["client_name"]
        cert_holder = cf.get("Certificate Holder") or client_name
    except Exception:
        client_name = audit["client_name"]
        cert_holder = client_name

    if not cert_holder or cert_holder == client_name:
        cert_holder_missing = True
    else:
        cert_holder_missing = False

    # Run analysis to get current sub status
    analysis = _compute_audit_analysis(audit)
    subs = analysis.get("subs", [])

    # Only request certs for subs that are NOT on file for the whole period
    # and that have a vendor_id (we know who they are)
    needs_cert = [
        s for s in subs
        if s["status"] != "full"
        and s["vendor_id"]
    ]

    no_contact: list[str] = []
    agent_groups: dict[str, dict] = {}  # agent_email → {contact, subs[]}
    direct_subs: list[dict] = []

    vendors_tbl = api.table(base_id, VENDORS_TABLE_ID)

    for sub in needs_cert:
        try:
            vrec = vendors_tbl.get(sub["vendor_id"])
            vf   = vrec["fields"]
        except Exception:
            no_contact.append(sub["payee_raw"])
            continue

        contact = _get_vendor_contact(vf)

        if contact["type"] == "none":
            no_contact.append(sub["payee_raw"])
            continue

        if contact["type"] == "agent":
            ce = contact["email"]
            if ce not in agent_groups:
                agent_groups[ce] = {"contact": contact, "subs": [], "vendor_ids": []}
            agent_groups[ce]["subs"].append(sub)
            agent_groups[ce]["vendor_ids"].append(sub["vendor_id"])
        else:
            direct_subs.append({"sub": sub, "contact": contact})

    drafts_created = 0

    # Create batched agent drafts
    for ce, grp in agent_groups.items():
        contact = grp["contact"]
        body = _build_audit_cert_request_body(
            contact=contact,
            client_name=client_name,
            cert_holder=cert_holder,
            subs=grp["subs"],
            audit_start=audit["start_date"],
            audit_end=audit["end_date"],
            coverage=audit["coverage"],
        )
        sub_names = ", ".join(s["vendor_name"] for s in grp["subs"][:3])
        if len(grp["subs"]) > 3:
            sub_names += f" + {len(grp['subs']) - 3} more"
        subject = (
            f"Certificate request — {client_name} — {audit['coverage']} audit — "
            f"{sub_names}"
        )
        em_rec = _create_email_message(
            subject=subject, body=body,
            to_email=ce,
            to_source="agent",
            items_covered=f"{len(grp['subs'])} subcontractors — {audit['coverage']} audit",
            vendor_ids=grp["vendor_ids"],
            client_ids=[audit["client_id"]],
            now_iso=now_iso,
        )
        if em_rec.get("id"):
            _astore.record_audit_email(audit_id, em_rec["id"], "cert_request_agent")
            drafts_created += 1

    # Direct-to-vendor drafts (no agent)
    for item in direct_subs:
        sub = item["sub"]
        contact = item["contact"]
        body = _build_audit_cert_request_body(
            contact=contact,
            client_name=client_name,
            cert_holder=cert_holder,
            subs=[sub],
            audit_start=audit["start_date"],
            audit_end=audit["end_date"],
            coverage=audit["coverage"],
        )
        subject = (
            f"Certificate request — {sub['vendor_name']} — {audit['coverage']} audit"
        )
        em_rec = _create_email_message(
            subject=subject, body=body,
            to_email=contact["email"],
            to_source="vendor",
            items_covered=f"{sub['vendor_name']} — {audit['coverage']}",
            vendor_ids=[sub["vendor_id"]],
            client_ids=[audit["client_id"]],
            now_iso=now_iso,
        )
        if em_rec.get("id"):
            _astore.record_audit_email(audit_id, em_rec["id"], "cert_request_vendor")
            drafts_created += 1

    # Draft to client for vendors with no contact
    no_contact_subs = [
        s for s in needs_cert if s["payee_raw"] in no_contact
    ] + [
        s for s in subs
        if s["status"] == "not_in_ccs"
    ]

    if no_contact_subs:
        lines = [
            "Hi,",
            "",
            f"As we work through the {audit['coverage']} audit for the period "
            f"{audit['start_date']} to {audit['end_date']}, I need help "
            f"reaching the following subcontractors. Could you send me their phone "
            f"number or email address?",
            "",
        ]
        for sub in no_contact_subs:
            paid_str = f"${sub['total_paid']:,.2f}" if sub.get("total_paid") else ""
            lines.append(
                f"  {sub['payee_raw']}"
                + (f" (paid {paid_str} in the period)" if paid_str else "")
            )
        lines += [
            "",
            "Thank you,",
            "Carolina Compliance Solutions",
        ]
        try:
            crec2 = api.table(base_id, CLIENTS_TABLE_ID).get(audit["client_id"])
            client_email = crec2["fields"].get("Client Email") or ""
        except Exception:
            client_email = ""

        subject_client = (
            f"Contact info needed — {audit['coverage']} audit subcontractors"
        )
        em_rec2 = _create_email_message(
            subject=subject_client, body="\n".join(lines),
            to_email=client_email,
            to_source="client",
            items_covered=f"{len(no_contact_subs)} subcontractors",
            client_ids=[audit["client_id"]],
            now_iso=now_iso,
        )
        if em_rec2.get("id"):
            _astore.record_audit_email(audit_id, em_rec2["id"], "contact_request_client")
            drafts_created += 1

    return jsonify({
        "ok": True,
        "drafts_created": drafts_created,
        "agent_count":    len(agent_groups),
        "direct_count":   len(direct_subs),
        "no_contact":     no_contact,
        "cert_holder_missing": cert_holder_missing,
        "emails_url": url_for("emails_page"),
    })


@app.route("/audits/<audit_id>/remediation", methods=["POST"])
@login_required
@csrf_protect
def audit_remediation(audit_id):
    """Save remediation note for a payee."""
    from flask import jsonify
    data = request.get_json(force=True) or {}
    payee_raw = (data.get("payee_raw") or "").strip()
    status    = (data.get("status") or "").strip()
    note      = (data.get("note") or "").strip()
    if not payee_raw:
        return jsonify({"ok": False, "error": "payee_raw required"}), 400
    _astore.upsert_remediation(audit_id, payee_raw, status, note)
    return jsonify({"ok": True})


@app.route("/audits/<audit_id>/update", methods=["POST"])
@login_required
@csrf_protect
def audit_update(audit_id):
    """Update audit metadata."""
    from flask import jsonify
    data = request.get_json(force=True) or {}
    allowed = {
        "status", "auditor_name", "policy_number", "notes", "due_date",
        "start_date", "end_date", "coverage",
    }
    updates = {k: v for k, v in data.items() if k in allowed}
    _astore.update_audit(audit_id, **updates)
    return jsonify({"ok": True})


@app.route("/audits/<audit_id>/build-pack", methods=["POST"])
@login_required
@csrf_protect
def audit_build_pack(audit_id):
    """Build an audit pack ZIP (Excel + PDF summary + cert PDFs)."""
    audit = _astore.get_audit(audit_id)
    if not audit:
        abort(404)

    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    api = _api()

    try:
        crec = api.table(base_id, CLIENTS_TABLE_ID).get(audit["client_id"])
        client_name = crec["fields"].get("Client Name") or audit["client_name"]
    except Exception:
        client_name = audit["client_name"]

    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
        HAS_OPENPYXL = True
    except ImportError:
        HAS_OPENPYXL = False

    analysis = _compute_audit_analysis(audit)
    subs      = analysis.get("subs", [])
    headline  = analysis.get("headline", {})
    all_packs = _astore.get_audit_packs(audit_id)
    version   = len(all_packs) + 1

    # Count open items for draft watermark
    open_items = [s for s in subs if s["status"] != "full"]
    has_open   = bool(open_items)

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:

        if HAS_OPENPYXL:
            wb = openpyxl.Workbook()

            # ── Sheet 1: Summary ──
            ws_sum = wb.active
            ws_sum.title = "Summary"
            _xl_bold = Font(bold=True)
            _xl_header_fill = PatternFill("solid", fgColor="102042")
            _xl_header_font = Font(bold=True, color="FFFFFF")

            sum_rows = [
                ("Audit", f"{client_name} — {audit['coverage']}"),
                ("Period", f"{audit['start_date']} to {audit['end_date']}"),
                ("Auditor / Insurer", audit.get("auditor_name") or ""),
                ("Policy Number", audit.get("policy_number") or ""),
                ("Generated", datetime.now().strftime("%m/%d/%Y")),
                ("Version", f"v{version}"),
                ("", ""),
                ("Subcontractor payees", headline.get("subs_count", 0)),
                ("Total paid to subs", f"${headline.get('total_paid', 0):,.2f}"),
                ("On file — whole period", headline.get("full_count", 0)),
                ("On file — part of period", headline.get("partial_count", 0)),
                ("Not on file", headline.get("missing_count", 0)),
                ("Not in CCS yet", headline.get("not_in_ccs_count", 0)),
                ("Paid during periods not on file", f"${headline.get('paid_in_gaps_total', 0):,.2f}"),
                ("", ""),
                ("", "Prepared by Carolina Compliance Solutions from certificates received. "
                    "This is a record of what is on file, not a statement about coverage "
                    "or an opinion on any insurer's or auditor's requirements."),
            ]
            if has_open:
                ws_sum.cell(row=1, column=1, value="DRAFT — OPEN ITEMS REMAIN").font = Font(
                    bold=True, color="FF0000", size=14
                )

            for i, (label, value) in enumerate(sum_rows, start=2 if has_open else 1):
                ws_sum.cell(row=i, column=1, value=label).font = _xl_bold
                ws_sum.cell(row=i, column=2, value=value)

            ws_sum.column_dimensions["A"].width = 32
            ws_sum.column_dimensions["B"].width = 50

            # ── Sheet 2: Subcontractors ──
            ws_subs = wb.create_sheet("Subcontractors")
            sub_headers = [
                "Payee Name", "Trade / Work Type", "CCS Vendor Match",
                "Total Paid", "Txn Count", "First Payment", "Last Payment",
                "Terms on File (Effective – Expiration)", "Days Not on File",
                "Paid During Periods Not on File", "Status", "Notes",
            ]
            for col, h in enumerate(sub_headers, 1):
                cell = ws_subs.cell(row=1, column=col, value=h)
                cell.font = _xl_header_font
                cell.fill = _xl_header_fill
                cell.alignment = Alignment(wrap_text=True)

            for row_i, s in enumerate(subs, 2):
                terms_str = "; ".join(
                    f"{t['effective']} to {t['expiration']}"
                    for t in s.get("terms", [])
                ) or "None on file"
                rem_note = s.get("remediation_note", "")
                if s.get("remediation"):
                    rem_note = f"[{s['remediation']}] {rem_note}".strip()

                ws_subs.cell(row=row_i, column=1,  value=s["payee_raw"])
                ws_subs.cell(row=row_i, column=2,  value=s.get("trade") or "")
                ws_subs.cell(row=row_i, column=3,  value=s["vendor_name"] if s.get("vendor_id") else "Not matched")
                ws_subs.cell(row=row_i, column=4,  value=s["total_paid"])
                ws_subs.cell(row=row_i, column=5,  value=s["txn_count"])
                ws_subs.cell(row=row_i, column=6,  value=s.get("first_date") or "")
                ws_subs.cell(row=row_i, column=7,  value=s.get("last_date") or "")
                ws_subs.cell(row=row_i, column=8,  value=terms_str)
                ws_subs.cell(row=row_i, column=9,  value=s["days_not_on_file"])
                ws_subs.cell(row=row_i, column=10, value=s["paid_in_gaps"])
                ws_subs.cell(row=row_i, column=11, value=s["status_label"])
                ws_subs.cell(row=row_i, column=12, value=rem_note)

            for col in range(1, len(sub_headers) + 1):
                ws_subs.column_dimensions[get_column_letter(col)].width = 18
            ws_subs.column_dimensions["A"].width = 28
            ws_subs.column_dimensions["H"].width = 40

            # ── Sheet 3: Payments ──
            ws_pay = wb.create_sheet("Payments")
            pay_headers = ["Date", "Payee", "Account", "Type", "Amount", "Card/Bank", "Included"]
            for col, h in enumerate(pay_headers, 1):
                cell = ws_pay.cell(row=1, column=col, value=h)
                cell.font = _xl_header_font
                cell.fill = _xl_header_fill

            all_payments = _astore.get_payments(
                audit_id, in_period_only=True,
                start_date=audit["start_date"], end_date=audit["end_date"],
            )
            # Build a set of sub payees (confirmed + unclear)
            sub_payees = {s["payee_raw"] for s in subs}
            for row_i, p in enumerate(all_payments, 2):
                ws_pay.cell(row=row_i, column=1, value=p["txn_date"])
                ws_pay.cell(row=row_i, column=2, value=p["payee_raw"])
                ws_pay.cell(row=row_i, column=3, value=p.get("account") or "")
                ws_pay.cell(row=row_i, column=4, value=p.get("txn_type") or "")
                ws_pay.cell(row=row_i, column=5, value=p["amount"])
                ws_pay.cell(row=row_i, column=6, value="Card" if p.get("is_card") else "Bank")
                ws_pay.cell(row=row_i, column=7, value="Yes" if p["payee_raw"] in sub_payees else "No (excluded)")

            # ── Sheet 4: Terms on File ──
            ws_terms = wb.create_sheet("Terms on File")
            term_headers = [
                "Payee", "CCS Vendor", "Coverage", "Policy Number",
                "Carrier", "Effective", "Expiration", "Received",
            ]
            for col, h in enumerate(term_headers, 1):
                cell = ws_terms.cell(row=1, column=col, value=h)
                cell.font = _xl_header_font
                cell.fill = _xl_header_fill

            trow = 2
            for s in subs:
                for t in s.get("terms", []):
                    ws_terms.cell(row=trow, column=1, value=s["payee_raw"])
                    ws_terms.cell(row=trow, column=2, value=s["vendor_name"])
                    ws_terms.cell(row=trow, column=3, value=audit["coverage"])
                    ws_terms.cell(row=trow, column=4, value=t.get("policy_number") or "")
                    ws_terms.cell(row=trow, column=5, value=t.get("carrier") or "")
                    ws_terms.cell(row=trow, column=6, value=t.get("effective") or "")
                    ws_terms.cell(row=trow, column=7, value=t.get("expiration") or "")
                    ws_terms.cell(row=trow, column=8, value=s.get("cert_received") or "")
                    trow += 1

            # Save workbook to ZIP
            xl_buf = io.BytesIO()
            wb.save(xl_buf)
            xl_buf.seek(0)
            zf.writestr("audit_workbook.xlsx", xl_buf.read())

        # ── PDF Summary (plain text for now) ──
        def _fd(s):
            d = _parse_date(s)
            return d.strftime("%m/%d/%Y") if d else s

        pdf_lines = [
            "AUDIT CERTIFICATE SUMMARY",
            "=" * 50,
            f"Client:    {client_name}",
            f"Coverage:  {audit['coverage']}",
            f"Period:    {_fd(audit['start_date'])} to {_fd(audit['end_date'])}",
            f"Auditor:   {audit.get('auditor_name') or '—'}",
            f"Generated: {datetime.now().strftime('%m/%d/%Y')}",
            f"Version:   v{version}",
            "",
        ]
        if has_open:
            pdf_lines.append("*** DRAFT — OPEN ITEMS REMAIN ***")
            pdf_lines.append("")

        pdf_lines += [
            "HEADLINE",
            f"  Subcontractor payees:               {headline.get('subs_count', 0)}",
            f"  Total paid to subs:                 ${headline.get('total_paid', 0):>12,.2f}",
            f"  On file — whole period:             {headline.get('full_count', 0)} (${headline.get('full_dollars', 0):,.2f})",
            f"  On file — part of period:           {headline.get('partial_count', 0)} (${headline.get('partial_dollars', 0):,.2f})",
            f"  Not on file:                        {headline.get('missing_count', 0)} (${headline.get('missing_dollars', 0):,.2f})",
            f"  Not in CCS yet:                     {headline.get('not_in_ccs_count', 0)} (${headline.get('not_in_ccs_dollars', 0):,.2f})",
            f"  Paid during periods not on file:    ${headline.get('paid_in_gaps_total', 0):>12,.2f}",
            "",
            "SUBCONTRACTORS",
            "-" * 50,
        ]
        for s in subs:
            status_lbl = s.get("status_label", s["status"])
            pdf_lines.append(f"  {s['payee_raw']}")
            pdf_lines.append(f"    Paid: ${s['total_paid']:,.2f}  |  {status_lbl}")
            if s.get("remediation"):
                pdf_lines.append(f"    Note: [{s['remediation']}] {s.get('remediation_note', '')}")
            pdf_lines.append("")

        pdf_lines += [
            "",
            "Prepared by Carolina Compliance Solutions from certificates received.",
            "This is a record of what is on file, not a statement about coverage",
            "or an opinion on any insurer's or auditor's requirements.",
        ]
        zf.writestr("audit_summary.txt", "\n".join(pdf_lines))

        # ── Cert PDFs (from Airtable attachments) ──
        cert_added = 0
        for s in subs:
            if not s.get("vendor_id") or not s.get("terms"):
                continue
            for t in s["terms"]:
                # Try to fetch source-page attachment from policy record
                pol_ids = _get_vendor_policy_ids(s["vendor_id"])
                if not pol_ids:
                    continue
                try:
                    pol_formula = "OR(" + ",".join(f"RECORD_ID()='{p}'" for p in pol_ids) + ")"
                    pols = api.table(base_id, INSURANCE_POLICIES_TABLE_ID).all(
                        formula=pol_formula,
                        fields=["Policy Number", SOURCE_PAGE_FIELD, "Policy Type",
                                "Effective Date", "Expiration Date"],
                    )
                    kws = _audit_coverage_keywords(audit["coverage"])
                    for pol in pols:
                        pf = pol["fields"]
                        if not _pol_matches_audit_type((pf.get("Policy Type") or "").lower(), kws):
                            continue
                        pnum = (pf.get("Policy Number") or "").strip()
                        if pnum and t.get("policy_number") and pnum != t["policy_number"]:
                            continue
                        attachments = pf.get(SOURCE_PAGE_FIELD) or []
                        for att in attachments:
                            att_url  = att.get("url") or ""
                            att_name = att.get("filename") or "cert.pdf"
                            if not att_url:
                                continue
                            try:
                                with urllib.request.urlopen(att_url, timeout=15) as resp:
                                    pdf_data = resp.read()
                                safe_vendor = re.sub(r"[^A-Za-z0-9 _-]", "", s["payee_raw"])[:30]
                                eff  = t.get("effective", "")[:10]
                                exp  = t.get("expiration", "")[:10]
                                covt = _audit_type_abbrev(audit["coverage"])
                                zip_name = f"certs/{safe_vendor} - {covt} - {eff} to {exp}.pdf"
                                zf.writestr(zip_name, pdf_data)
                                cert_added += 1
                            except Exception:
                                pass
                except Exception:
                    pass

    zip_buf.seek(0)
    pack_path = os.path.join(_AUDIT_UPLOADS_DIR, f"pack_{audit_id}_v{version}.zip")
    with open(pack_path, "wb") as fh:
        fh.write(zip_buf.getvalue())

    pack = _astore.create_audit_pack(audit_id, pack_path, has_open_items=has_open)
    _astore.update_audit(audit_id, status="Pack built")

    # Update Airtable record
    if audit.get("airtable_id"):
        try:
            api.table(base_id, AUDITS_TABLE_ID).update(audit["airtable_id"], {
                "Audit Type": audit["coverage"],
            })
            _hs.record_write("Audits", 1)
        except Exception:
            pass

    zip_buf.seek(0)
    safe_client = re.sub(r'[\\/:*?"<>|]', "_", client_name)
    safe_cov    = re.sub(r'[\\/:*?"<>|]', "_", audit["coverage"])
    start_s     = (audit["start_date"] or "")[:10].replace("-", "")
    end_s       = (audit["end_date"] or "")[:10].replace("-", "")
    dl_name     = f"{safe_client} - {safe_cov} - {start_s} to {end_s} - v{version}.zip"

    from flask import send_file
    return send_file(
        zip_buf,
        as_attachment=True,
        download_name=dl_name,
        mimetype="application/zip",
    )


@app.route("/audits/<audit_id>/checklist")
@login_required
def audit_checklist(audit_id):
    """Return checklist JSON for the audit workspace progress bar."""
    from flask import jsonify
    audit = _astore.get_audit(audit_id)
    if not audit:
        return jsonify({"ok": False, "error": "Not found"}), 404

    files = _astore.get_audit_files(audit_id)
    cls_all = _astore.get_payee_classifications(audit_id)
    packs = _astore.get_audit_packs(audit_id)

    has_file = any(f["parse_status"] == "ok" for f in files)
    unconfirmed = [
        c for c in cls_all
        if not c.get("confirmed") and c.get("payee_group") not in ("not_sub",)
    ]
    unclear = [c for c in cls_all if c.get("payee_group") == "unclear"]
    no_trade = [
        c for c in cls_all
        if c.get("payee_group") in ("ccs_vendor", "likely_sub")
        and not c.get("trade")
    ]

    email_ids = _astore.get_audit_email_ids(audit_id)
    drafts_exist = bool(email_ids)

    checks = [
        {
            "step": 1,
            "title": "Upload payments file",
            "done": has_file,
            "detail": (
                f"{files[0]['row_count']:,} payments, "
                f"{files[0]['date_min']} to {files[0]['date_max']}"
                if has_file and files else "Drop a QuickBooks Transaction List CSV"
            ),
        },
        {
            "step": 2,
            "title": "Confirm payees",
            "done": has_file and not unconfirmed,
            "detail": (
                f"{len(unconfirmed)} payees need a decision"
                if unconfirmed else "All payees confirmed"
            ),
            "count": len(unconfirmed),
        },
        {
            "step": 3,
            "title": "Confirm work types",
            "done": has_file and not no_trade,
            "detail": (
                f"{len(no_trade)} subcontractors missing a trade / work type"
                if no_trade else "All work types set"
            ),
            "count": len(no_trade),
        },
        {
            "step": 4,
            "title": "Review queue",
            "done": True,  # Always available
            "detail": "Check for certificates pending approval",
            "link": url_for("queue"),
        },
        {
            "step": 5,
            "title": "Request missing certificates",
            "done": drafts_exist,
            "detail": (
                f"{len(email_ids)} drafts created"
                if drafts_exist else "No drafts yet"
            ),
            "link": url_for("emails_page"),
        },
        {
            "step": 6,
            "title": "Follow up on requests",
            "done": False,
            "detail": "Check cadence — view Emails page for outstanding requests",
            "link": url_for("emails_page"),
        },
        {
            "step": 7,
            "title": "Handle the remainder",
            "done": False,
            "detail": "Mark payees as requested/no response, not a sub, or other",
        },
        {
            "step": 8,
            "title": "Build audit pack",
            "done": bool(packs),
            "detail": (
                f"v{packs[0]['version']} built {datetime.fromtimestamp(packs[0]['created_at']).strftime('%m/%d/%Y')}"
                if packs else "Not built yet"
            ),
        },
    ]

    done_count  = sum(1 for c in checks if c["done"])
    total_count = len(checks)
    next_step   = next((c for c in checks if not c["done"]), checks[-1])

    return jsonify({
        "ok":          True,
        "steps":       checks,
        "done_count":  done_count,
        "total_count": total_count,
        "next_step":   next_step,
    })


# ── Marketing ────────────────────────────────────────────────────────────────

from marketing_constants import (
    MARKETING_BASE_ID,
    MARKETING_IDEAS_TABLE_ID, MI_IDEA, MI_WHY_STORY, MI_AUDIENCE, MI_TOPIC,
    MI_STATUS, MI_SIGNAL_LINK, MI_STATUS_NEW, MI_STATUS_APPROVED, MI_STATUS_DISMISSED,
    MARKETING_SIGNALS_TABLE_ID, MS_SUMMARY, MS_SOURCE, MS_CREATED_AT,
    CONTENT_QUEUE_TABLE_ID, CQ_DRAFT_TEXT, CQ_PLATFORM, CQ_HOOKS, CQ_STATUS,
    CQ_NEEDS_BILLY, CQ_GUARD_RESULT, CQ_TRACKED_LINK, CQ_IDEA_LINK,
)


def _load_content_queue(api) -> dict:
    """Return a dict mapping idea_id -> list of content queue dicts."""
    result = {}
    try:
        tbl = api.table(MARKETING_BASE_ID, CONTENT_QUEUE_TABLE_ID)
        records = tbl.all(
            formula=f"{{{CQ_STATUS}}}='Draft'",
            fields=[CQ_DRAFT_TEXT, CQ_PLATFORM, CQ_HOOKS, CQ_GUARD_RESULT,
                    CQ_TRACKED_LINK, CQ_NEEDS_BILLY, CQ_IDEA_LINK],
        )
        for rec in records:
            f = rec.get("fields", {})
            for idea_id in (f.get(CQ_IDEA_LINK) or []):
                result.setdefault(idea_id, []).append({
                    "id":           rec["id"],
                    "platform":     f.get(CQ_PLATFORM, ""),
                    "draft_text":   f.get(CQ_DRAFT_TEXT, ""),
                    "hooks":        f.get(CQ_HOOKS, ""),
                    "guard_result": f.get(CQ_GUARD_RESULT, "PASS"),
                    "tracked_link": f.get(CQ_TRACKED_LINK, ""),
                    "needs_billy":  f.get(CQ_NEEDS_BILLY, False),
                })
    except Exception as exc:
        app.logger.warning("content queue load failed: %s", exc)
    return result


@app.route("/marketing")
@login_required
def marketing_page():
    ideas = []
    error = None
    try:
        api = _api()
        tbl = api.table(MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID)
        records = tbl.all(
            formula="OR({Status}='New',{Status}='Approved')",
            sort=[{"field": MI_STATUS, "direction": "asc"}],
        )
        content_map = _load_content_queue(api)
        for rec in records:
            f = rec.get("fields", {})
            idea_id = rec["id"]
            ideas.append({
                "id":        idea_id,
                "idea":      f.get(MI_IDEA, ""),
                "why_story": f.get(MI_WHY_STORY, ""),
                "audience":  f.get(MI_AUDIENCE, ""),
                "topic":     f.get(MI_TOPIC, ""),
                "status":    f.get(MI_STATUS, MI_STATUS_NEW),
                "content":   content_map.get(idea_id, []),
            })
    except Exception as exc:
        app.logger.warning("marketing_page ideas fetch failed: %s", exc)
        error = "Could not load ideas — Airtable may be unavailable."

    return render_template("marketing.html", ideas=ideas, error=error)


@app.route("/marketing/idea/<record_id>/approve", methods=["POST"])
@login_required
def marketing_approve_idea(record_id):
    try:
        api = _api()
        tbl = api.table(MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID)
        tbl.update(record_id, {MI_STATUS: MI_STATUS_APPROVED})
    except Exception as exc:
        app.logger.warning("marketing approve failed: %s", exc)
        flash("Could not approve idea.", "warning")
    return redirect(url_for("marketing_page"))


@app.route("/marketing/idea/<record_id>/dismiss", methods=["POST"])
@login_required
def marketing_dismiss_idea(record_id):
    try:
        api = _api()
        tbl = api.table(MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID)
        tbl.update(record_id, {MI_STATUS: MI_STATUS_DISMISSED})
    except Exception as exc:
        app.logger.warning("marketing dismiss failed: %s", exc)
        flash("Could not dismiss idea.", "warning")
    return redirect(url_for("marketing_page"))


@app.route("/marketing/note", methods=["POST"])
@login_required
def marketing_add_note():
    note = (request.form.get("note") or "").strip()
    if note:
        try:
            api = _api()
            tbl = api.table(MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID)
            tbl.create({
                MI_IDEA: note,
                MI_WHY_STORY: "Quick note from Haley",
                MI_AUDIENCE: "GC",
                MI_TOPIC: "Certificate mistakes",
                MI_STATUS: MI_STATUS_NEW,
            })
            flash("Note saved.", "success")
        except Exception as exc:
            app.logger.warning("marketing note save failed: %s", exc)
            flash("Could not save note.", "warning")
    return redirect(url_for("marketing_page"))


# ── Internal pulse endpoints (no login_required — auth via Bearer token) ─────

@app.route("/internal/pulse-checkin", methods=["POST"])
def internal_pulse_checkin():
    """Receive health events and heartbeats from the cron-pulse Railway service."""
    from flask import jsonify
    token = os.environ.get("PULSE_SECRET_TOKEN", "").strip()
    if not token or request.headers.get("Authorization", "") != f"Bearer {token}":
        return jsonify({"ok": False}), 401
    data = request.get_json(force=True) or {}
    for svc in (data.get("heartbeats") or []):
        _hs.record_heartbeat(str(svc))
    for ev in (data.get("events") or []):
        _hs.record_event(
            category=ev.get("category", "pipeline"),
            level=ev.get("level", "ok"),
            title=ev.get("title", ""),
            body=ev.get("body", ""),
            table_name=ev.get("table_name", ""),
        )
    if data.get("prune"):
        _hs.prune_old_data(keep_days=7)
    return jsonify({"ok": True})


@app.route("/internal/health-data", methods=["GET"])
def internal_health_data():
    """Return current health data for the cron-pulse service to read."""
    from flask import jsonify
    token = os.environ.get("PULSE_SECRET_TOKEN", "").strip()
    if not token or request.headers.get("Authorization", "") != f"Bearer {token}":
        return jsonify({"ok": False}), 401
    heartbeats = _hs.get_all_heartbeats()
    tables = ["Compliance Log", "Insurance Policies", "Incoming Extractions"]
    write_rates = {t: _hs.get_write_rate(t, window_minutes=60) for t in tables}
    recent_events = _hs.get_recent_events(limit=50, since_hours=25)
    return jsonify({"heartbeats": heartbeats, "write_rates": write_rates, "recent_events": recent_events})


@app.route("/marketing/idea/<record_id>/generate", methods=["POST"])
@login_required
def marketing_generate_content(record_id):
    try:
        api = _api()
        idea_tbl = api.table(MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID)
        rec = idea_tbl.get(record_id)
        if not rec:
            flash("Idea not found.", "warning")
            return redirect(url_for("marketing_page"))
        idea_fields = rec.get("fields", {})
        if idea_fields.get(MI_STATUS) != MI_STATUS_APPROVED:
            flash("Only approved ideas can generate a content set.", "warning")
            return redirect(url_for("marketing_page"))
        needs_billy = request.form.get("needs_billy") == "1"
    except Exception as exc:
        app.logger.warning("marketing generate fetch failed: %s", exc)
        flash("Could not load idea.", "warning")
        return redirect(url_for("marketing_page"))

    try:
        from marketing_content_generator import generate_content_set
        result = generate_content_set(record_id, idea_fields, needs_billy=needs_billy)
        created = result.get("created", 0)
        blocked = result.get("blocked", 0)
        if created:
            msg = f"Generated {created} content draft{'s' if created != 1 else ''}."
            if blocked:
                msg += f" {blocked} blocked by compliance guard."
            flash(msg, "success")
        else:
            flash("No content generated — check logs.", "warning")
    except Exception as exc:
        app.logger.warning("marketing content generation failed: %s", exc)
        flash("Content generation failed — check logs.", "warning")

    return redirect(url_for("marketing_page"))


if __name__ == "__main__":
    app.run(debug=True, port=5050)

