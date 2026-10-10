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
import time
import traceback
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

CORRECTIONS_TABLE_ID = "tblh6NTQikjzl8FFu"
EXTRACTIONS_TABLE_ID = "tblT88Ty6d6M766oY"  # Incoming Extractions — for Airtable deep-links

_BILLY_RTR_PREFIXES = ("COI_forms_", "RTR_COI_", "N9WC", "coi.json")

def _is_billy_rtr(source_filename: str) -> bool:
    """True if the filename belongs to the Billy/RTR real-client batch."""
    fn = source_filename or ""
    return (
        any(fn.startswith(p) for p in _BILLY_RTR_PREFIXES)
        or "resend" in fn.lower()
    )
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

_https_mode = os.environ.get("FLASK_HTTPS", "").lower() in ("1", "true", "yes")
app.config["SESSION_COOKIE_SECURE"] = _https_mode
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"


# ── CSRF ─────────────────────────────────────────────────────────────────────

def _csrf_token() -> str:
    """Return (and create if needed) the per-session CSRF token."""
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]

app.jinja_env.globals["csrf_token"] = _csrf_token


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


# ── Flag computation ─────────────────────────────────────────────────────────

_MC_ENTITY = r"(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Co\.?|Ltd\.?|PLLC|P\.C\.|LLP|LP)"
_MC_SEP    = r"(?:[\r\n]+|[ \t]+and[ \t]+|[ \t]*&[ \t]*|[ \t]+dba[ \t]+)"
_MULTI_COMPANY_RE = re.compile(
    _MC_ENTITY + r"[,.]?\s*" + _MC_SEP + r".*?" + _MC_ENTITY,
    re.IGNORECASE | re.DOTALL,
)


def compute_flags(airtable_fields: dict, raw_data: dict) -> dict:
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
    flags["missing_fields"] = any(
        isinstance(p, dict)
        and any(str(p.get(k) or "").strip() for k in _content_keys)
        and (
            not (p.get("policy_number") or "").strip()
            or (
                not (p.get("effective_date") or "").strip()
                and not (p.get("expiration_date") or "").strip()
            )
        )
        for p in policies
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
                        if bool(cp.get(chk)) != bool(pp.get(chk)):
                            flags["changed_vs_prior"] = True
                            break
                    if flags["changed_vs_prior"]:
                        break
        except (json.JSONDecodeError, AttributeError):
            pass

    # 6. Multi-company named insured
    ni = raw_data.get("named_insured") or ""
    flags["multi_company"] = bool(_MULTI_COMPANY_RE.search(ni))

    flags["needs_look"] = any(v for k, v in flags.items() if k != "needs_look")
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
        ("additional_insured_checked", bool),
        ("waiver_of_subrogation_checked", bool),
        ("primary_noncontributory_checked", bool),
        ("carrier", lambda v: str(v or "").strip().lower()),
    ]
    for i, (cp, sp) in enumerate(zip(cur_pols, sec_pols)):
        for field, norm in check_fields:
            if norm(cp.get(field)) != norm(sp.get(field)):
                highlights.add((i, field))
    return highlights


# ── Queue ─────────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return redirect(url_for("queue"))


@app.route("/queue")
@login_required
def queue():
    import re as _re
    from collections import Counter

    chip = request.args.get("filter", "pending")
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
        flags = compute_flags(f, raw_data)

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
        })

    PENDING_SET = {REVIEW_STATUS_PENDING_REVIEW}
    APPROVED_SET = {REVIEW_STATUS_APPROVED, REVIEW_STATUS_APPROVED_EDITED, REVIEW_STATUS_AUTO_APPROVED}
    REJECTED_SET = {REVIEW_STATUS_REJECTED}

    # Compute possible_duplicate flag at queue level (requires cross-row comparison)
    def _norm_ni(name: str) -> str:
        return _re.sub(r"\s+", " ", _re.sub(r"[^a-z0-9 ]", "", (name or "").lower())).strip()

    ni_counts = Counter(
        _norm_ni(r["named_insured"])
        for r in rows
        if r["named_insured"] and r["review_status"] in PENDING_SET
    )
    for r in rows:
        ni_key = _norm_ni(r["named_insured"])
        r["flags"]["possible_duplicate"] = (
            bool(ni_key) and ni_counts[ni_key] > 1
            and r["review_status"] in PENDING_SET
        )
        # Update needs_look to include possible_duplicate
        if r["flags"]["possible_duplicate"]:
            r["flags"]["needs_look"] = True

    # Batch filter
    batch = request.args.get("batch", "rtr")
    if batch == "rtr":
        batch_rows = [r for r in rows if _is_billy_rtr(r["source_filename"])]
    else:
        batch_rows = [r for r in rows if not _is_billy_rtr(r["source_filename"])]

    if chip == "ai_disagreed":
        filtered = [r for r in batch_rows if r["flags"]["ai_disagreed"] and r["review_status"] in PENDING_SET]
    elif chip == "needs_look":
        filtered = [r for r in batch_rows if r["flags"]["needs_look"] and r["review_status"] in PENDING_SET]
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

    return render_template("queue.html", rows=filtered, chip=chip, counts=counts, batch=batch)


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
    attachments = f.get(SOURCE_PAGE_FIELD)
    if isinstance(attachments, list) and attachments:
        source_page_url = attachments[0].get("url")

    ai_raw = (
        f.get("AI Disagreements") or f.get(AI_DISAGREEMENTS_FIELD_ID) or ""
    )
    ai_disagrees = [d.strip() for d in str(ai_raw).split("\n") if d.strip()] if ai_raw else []

    flags = compute_flags(f, raw_data)
    corrections_logged = request.args.get("corrections_logged", type=int, default=None)

    _base_id = clean_base_id(config.AIRTABLE_BASE_ID) if config.AIRTABLE_BASE_ID else ""
    airtable_url = (
        f"https://airtable.com/{_base_id}/{EXTRACTIONS_TABLE_ID}/{record_id}"
        if _base_id else ""
    )

    return render_template(
        "detail.html",
        record_id=record_id,
        f=f,
        raw_data=raw_data,
        second_data=second_data,
        highlight_cells=highlight_cells,
        prior_data=prior_data,
        source_page_url=source_page_url,
        ai_disagrees=ai_disagrees,
        flags=flags,
        corrections_logged=corrections_logged,
        airtable_url=airtable_url,
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

    if action_type == "approve":
        corrections_count = _handle_approve(
            table, record_id, source_filename, original_data, why_note, now_iso,
        )
        return redirect(url_for("detail", record_id=record_id,
                                corrections_logged=corrections_count))

    if action_type == "reject":
        table.update(record_id, {"Review Status": REVIEW_STATUS_REJECTED}, typecast=True)
        if why_note:
            _write_correction(source_filename, record_id, "Review Action",
                              "Pending Review", "Rejected", why_note, now_iso)
        return redirect(url_for("queue"))

    if action_type == "escalate":
        table.update(record_id, {"Review Status": REVIEW_STATUS_ESCALATED}, typecast=True)
        if why_note:
            _write_correction(source_filename, record_id, "Review Action",
                              "Pending Review", "Escalated to GC", why_note, now_iso)
        return redirect(url_for("queue"))

    abort(400)


def _handle_approve(table, record_id, source_filename, original_data, why_note, now_iso):
    """Apply edits, write Corrections rows, set Review Status. Returns corrections count."""
    corrections = []  # list of (field_path, old_val, new_val)

    # Top-level text fields
    for field in ("named_insured", "certificate_holder"):
        submitted = (request.form.get(field) or "").strip()
        original = (original_data.get(field) or "").strip()
        if submitted != original:
            corrections.append((field, original, submitted))

    # Policy table
    original_policies = original_data.get("policies") or []
    new_policies = []
    for i, orig in enumerate(original_policies):
        new_pol = dict(orig)
        for pf in ("policy_type", "policy_number", "carrier",
                   "effective_date", "expiration_date",
                   "coverage_limits", "policy_basis"):
            submitted = (request.form.get(f"policy_{i}_{pf}") or "").strip() or None
            original_val = (orig.get(pf) or None)
            if isinstance(original_val, str):
                original_val = original_val.strip() or None
            if submitted != original_val:
                corrections.append((f"policies[{i}].{pf}", str(original_val or ""), str(submitted or "")))
            new_pol[pf] = submitted if submitted is not None else orig.get(pf)
        for chk in ("additional_insured_checked",
                    "waiver_of_subrogation_checked",
                    "primary_noncontributory_checked"):
            submitted_bool = request.form.get(f"policy_{i}_{chk}") == "true"
            original_bool = bool(orig.get(chk))
            if submitted_bool != original_bool:
                corrections.append((f"policies[{i}].{chk}", str(original_bool), str(submitted_bool)))
            new_pol[chk] = submitted_bool
        new_policies.append(new_pol)

    # Write Corrections rows
    for field_path, old_val, new_val in corrections:
        _write_correction(source_filename, record_id, field_path, old_val, new_val, why_note, now_iso)

    review_status = REVIEW_STATUS_APPROVED_EDITED if corrections else REVIEW_STATUS_APPROVED

    update = {"Review Status": review_status}
    _airtable_field_names = {
        "named_insured": "Named Insured",
        "certificate_holder": "Certificate Holder",
    }
    if corrections:
        new_data = dict(original_data)
        new_data["policies"] = new_policies
        for fp, old, new in corrections:
            if fp in _airtable_field_names:
                new_data[fp] = new
                update[_airtable_field_names[fp]] = new
        update["Raw JSON"] = json.dumps(new_data, indent=2)

    table.update(record_id, update, typecast=True)
    return len(corrections)


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


if __name__ == "__main__":
    app.run(debug=True, port=5050)
