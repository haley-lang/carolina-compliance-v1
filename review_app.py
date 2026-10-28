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
import json
import os
import re
import secrets
import subprocess
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps

from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, abort, flash, redirect, render_template,
    request, session, url_for,
)

import config
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

_BILLY_RTR_PREFIXES = ("COI_forms_", "RTR_COI_", "N9WC", "coi.json")

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
            or issue.get("type") in ("non_coi", "possible_duplicate")
            or issue.get("id") in ("ai_named_insured", "possible_duplicate")
            or "missing_required" in (issue.get("id") or "")
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


def csrf_protect(f):
    """Decorator: reject POST requests that don't carry the correct session CSRF token."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if request.method == "POST":
            submitted = request.form.get("csrf_token", "")
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
        def _run():
            try:
                subprocess.run([_script, _proc_py], cwd=os.path.dirname(__file__), timeout=300)
            except Exception as exc:
                app.logger.warning("processor.py subprocess error: %s", exc)
        threading.Thread(target=_run, daemon=True).start()
    except Exception as exc:
        app.logger.error("_trigger_post_confirm failed for %s: %s", record_id, exc, exc_info=True)


def _get_vendor_policies(vendor_record_id: str) -> list:
    """Return all Insurance Policy records linked to a vendor, sorted by type then expiry."""
    base_id = clean_base_id(config.AIRTABLE_BASE_ID)
    tbl = _api().table(base_id, INSURANCE_POLICIES_TABLE_ID)
    formula = f"FIND('{vendor_record_id}', ARRAYJOIN({{Vendor Link}}, ','))"
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

    # changed_vs_prior is an informational note, not an action trigger
    flags["needs_look"] = any(
        v for k, v in flags.items()
        if k not in ("needs_look", "changed_vs_prior")
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

    return issues


# ── Queue ─────────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return redirect(url_for("queue"))


@app.route("/queue")
@login_required
def queue():
    chip = request.args.get("filter", "needs_decision")
    table = _ie_table()
    all_records = table.all()

    rows = []
    for rec in all_records:
        f = rec["fields"]
        try:
            raw_data = json.loads(f.get("Raw JSON") or "{}")
        except json.JSONDecodeError:
            raw_data = {}

        review_status = (f.get("Review Status") or "").strip()
        _rpt = RTR_REQUIRED_POLICY_TYPES if _is_billy_rtr(f.get("Source Filename") or "") else None
        flags = compute_flags(f, raw_data, _rpt)

        rows.append({
            "id": rec["id"],
            "source_filename": f.get("Source Filename") or "",
            "named_insured": f.get("Named Insured") or "",
            "review_status": review_status,
            "review_reason": f.get("Review Reason") or "",
            "confidence": f.get("Confidence Score"),
            "policies_count": f.get("Policies Count") or 0,
            "extraction_at": f.get("Extraction Processed At") or "",
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

    if chip == "needs_decision":
        filtered = [r for r in batch_rows if r["flags"]["needs_decision"] and r["review_status"] in PENDING_SET]
    elif chip == "just_confirm":
        filtered = [r for r in batch_rows if not r["flags"]["needs_decision"] and r["review_status"] in PENDING_SET]
    elif chip == "approved":
        filtered = [r for r in batch_rows if r["review_status"] in APPROVED_SET]
    elif chip == "rejected":
        filtered = [r for r in batch_rows if r["review_status"] in REJECTED_SET]
    else:
        filtered = [r for r in batch_rows if r["review_status"] in PENDING_SET]

    # Step 1: sort by extraction_at descending (newest first)
    filtered.sort(key=lambda r: r["extraction_at"], reverse=True)
    # Step 2: stable-sort by priority group (preserves newest-first within each group)
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
        "needs_look": sum(1 for r in batch_rows if r["flags"]["needs_look"] and r["review_status"] in PENDING_SET),
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

    weekly_learnings = _get_weekly_learnings()
    return render_template("queue.html", rows=filtered, chip=chip, counts=counts, batch=batch,
                           weekly_learnings=weekly_learnings)


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

    update = {"Review Status": review_status}
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


if __name__ == "__main__":
    app.run(debug=True, port=5050)
