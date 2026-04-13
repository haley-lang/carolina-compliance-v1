"""
Module 2 — COI Document Extractor
Processes all pending files from the uploads/ folder, sends each to Anthropic
Claude for structured data extraction, and saves results as JSON in extracted/.
Skips files that already have a corresponding JSON output in extracted/.
"""

import base64
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
import anthropic

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "uploads"))
EXTRACT_DIR = Path("extracted")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SUPPORTED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png"}

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CANCELLATION_KEYWORDS = (
    "notice of cancellation",
    "cancellation",
    "cancelled",
    "reinstatement",
    "reinstated",
    "notice of reinstatement",
    "policy reinstated",
)

# ── Extraction prompt ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a document analysis assistant specializing in insurance documents.
Extract structured data from the provided document image(s) and return ONLY valid JSON — no prose, no markdown fences.

Rules:
- Be conservative. Only extract data that is clearly visible in the document.
- If a field cannot be found, return null (for scalar fields) or [] (for arrays).
- Do not infer, guess, or fabricate policy numbers, dates, or coverage limits.
- If the document type is ambiguous or unrecognizable, set document_type to "unknown".
- For contact_emails, include any email addresses visible anywhere in the document.
- For ACORD forms, extract certificate_holder from the "CERTIFICATE HOLDER" section (bottom-left of the form).
- certificate_holder is usually the company/entity receiving the certificate.
- Do NOT confuse certificate_holder with named_insured.
- Return certificate_holder as a plain string.
- If certificate_holder is blank or not clearly present, return null.
- Do not infer or guess certificate_holder.
- ACORD POLICY TABLE EXTRACTION (row-locked):
- Identify each non-blank ACORD policy coverage row first (e.g., CGL, AUTO, UMBRELLA, WC).
- For each identified row, read values LEFT-TO-RIGHT from that SAME row and map:
  carrier -> policy_number -> effective_date -> expiration_date -> coverage_limits.
- Treat policy_number, effective_date, and expiration_date as required row-level targets: re-check each cell before returning null.
- Do NOT copy policy_number or dates from a neighboring row (no row bleed).
- If text is faint or split, combine obvious OCR fragments within the same cell (remove extra spaces only).
- policy_number: return exact visible alphanumeric text (including hyphens/slashes).
- If a policy number field contains 'TBD', 'tbd', 'to be determined', 'pending', or any variation indicating the policy number is not yet assigned, set policy_number to null and set policy_number_invalid to true. Do not treat TBD as a valid policy number under any circumstances.
- effective_date and expiration_date: return visible date text; prefer MM/DD/YYYY when clear.
- Only return null for these fields when the specific row cell is truly blank or unreadable.
- Keep the policy row even if some fields are null.
- coverage_limits should preserve labeled COI sublimits when shown in the document.
- Prefer uppercase, comma-separated label/value pairs when labels are visible (e.g. "EACH OCCURRENCE $1,000,000, GENERAL AGGREGATE $2,000,000").
- If only unlabeled numeric limits are visible, return the plain numeric text as shown (e.g. "$1,000,000 / $2,000,000").
- CLAIMS-MADE vs OCCURRENCE:
- Check if the ACORD form has the "CLAIMS-MADE" or "OCCUR" checkbox marked for each policy row.
- Set policy_basis to "claims-made" if CLAIMS-MADE is checked, "occurrence" if OCCUR is checked, null if neither is clearly marked.
- RETROACTIVE DATE: For claims-made policies, look for a retroactive date (often labeled "RETRO DATE" or "RET. DATE" near the claims-made checkbox). Return as retro_date in MM/DD/YYYY format. Return null if not visible.
- TAIL COVERAGE: If the document mentions "extended reporting period", "tail coverage", "ERP", or similar language, set tail_coverage_evidenced to true. Otherwise set to false.
- ENDORSEMENT CHECKBOXES: For each policy row on the ACORD form, check:
  - additional_insured_checked: true if the "ADDL INSD" or "Additional Insured" checkbox/column is marked Y or checked for that row. false otherwise.
  - waiver_of_subrogation_checked: true if the "SUBR WVD" or "Waiver of Subrogation" checkbox/column is marked Y or checked for that row. false otherwise.
  - primary_noncontributory_checked: true if "Primary and Noncontributory" or "Primary/Non-Contributory" language appears in the description of operations referencing this policy type. false otherwise.
- DESCRIPTION OF OPERATIONS: Extract the full text from the "DESCRIPTION OF OPERATIONS / LOCATIONS / VEHICLES" section at the bottom of the ACORD form. Return as description_of_operations at the top level. Return null if blank or not present.

Return this exact JSON structure:
{
  "document_type": "<COI | cancellation_notice | endorsement | reinstatement | unknown>",
  "named_insured": "<string or null>",
  "certificate_holder": "<string or null>",
  "certificate_date": "<string or null>",
  "contact_emails": ["<email>"],
  "description_of_operations": "<string or null>",
  "policies": [
    {
      "policy_type": "<string or null>",
      "policy_number": "<string or null>",
      "policy_number_invalid": false,
      "carrier": "<string or null>",
      "effective_date": "<string or null>",
      "expiration_date": "<string or null>",
      "coverage_limits": "<string or null>",
      "policy_basis": "<claims-made | occurrence | null>",
      "retro_date": "<string or null>",
      "tail_coverage_evidenced": false,
      "additional_insured_checked": false,
      "waiver_of_subrogation_checked": false,
      "primary_noncontributory_checked": false
    }
  ]
}"""

SECOND_PASS_POLICY_PROMPT = """You are a document analysis assistant specializing in insurance documents.
Return ONLY valid JSON — no prose, no markdown fences.

FOCUSED POLICY FIELD EXTRACTION:
- Ignore everything except the ACORD policy table
- For each visible coverage row:
  extract:
    policy_number
    effective_date
    expiration_date
- Look carefully at small text columns
- Re-scan each row multiple times before returning null
- Combine fragmented OCR text when needed
- Return values even if slightly uncertain (do NOT default to null unless truly unreadable)
- If a policy number field contains 'TBD', 'tbd', 'to be determined', 'pending', or any variation indicating the policy number is not yet assigned, set policy_number to null and set policy_number_invalid to true. Do not treat TBD as a valid policy number under any circumstances.

Return this exact JSON structure:
{
  "policies": [
    {
      "policy_number": "<string or null>",
      "policy_number_invalid": false,
      "effective_date": "<string or null>",
      "expiration_date": "<string or null>"
    }
  ]
}"""

DEDICATED_ACORD_POLICY_TABLE_PROMPT = """You are a document analysis assistant specializing in ACORD 25 policy-table extraction.
Return ONLY valid JSON — no prose, no markdown fences.

Read the ACORD policy table row by row.
Identify each visible coverage row.
For each same row only, extract:
- policy_type
- policy_number
- effective_date
- expiration_date
- coverage_limits

Rules:
- Never borrow values from neighboring rows.
- Return null only when a specific cell is truly unreadable.
- Preserve visible text exactly for policy_number, effective_date, and expiration_date.
- If a policy number field contains 'TBD', 'tbd', 'to be determined', 'pending', or any variation indicating the policy number is not yet assigned, set policy_number to null and set policy_number_invalid to true. Do not treat TBD as a valid policy number under any circumstances.

Return this exact JSON structure:
{
  "policies": [
    {
      "policy_type": "<string or null>",
      "policy_number": "<string or null>",
      "policy_number_invalid": false,
      "effective_date": "<string or null>",
      "expiration_date": "<string or null>",
      "coverage_limits": "<string or null>"
    }
  ]
}"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_all_pending_files(folder: Path) -> list[Path]:
    """Return all supported files in the folder that haven't been extracted yet.

    A file is considered already extracted if a JSON with the same stem
    exists in EXTRACT_DIR.  Results are sorted oldest-first so files are
    processed in the order they arrived.
    """
    if not folder.exists():
        raise FileNotFoundError(f"Upload folder not found: {folder}")

    candidates = [
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    if not candidates:
        return []

    # Skip files that already have a corresponding extracted JSON
    pending = []
    for f in candidates:
        json_path = EXTRACT_DIR / (f.stem + ".json")
        if json_path.exists():
            log.debug("Skipping %s — already extracted (%s)", f.name, json_path.name)
        else:
            pending.append(f)

    # Process oldest files first (FIFO by modification time)
    pending.sort(key=lambda f: f.stat().st_mtime)
    return pending


def pdf_to_base64_images(pdf_path: Path) -> list[str]:
    """Convert each PDF page to a base64-encoded PNG string."""
    try:
        from pdf2image import convert_from_path
    except ImportError:
        raise ImportError(
            "pdf2image is required for PDF support. "
            "Install it with: pip install pdf2image\n"
            "You also need poppler installed: brew install poppler"
        )

    import io
    log.info("Converting PDF to images (%s)…", pdf_path.name)
    pages = convert_from_path(str(pdf_path), dpi=200)
    log.info("PDF has %d page(s)", len(pages))

    encoded = []
    for i, page in enumerate(pages, start=1):
        buffer = io.BytesIO()
        page.save(buffer, format="PNG")
        encoded.append(base64.b64encode(buffer.getvalue()).decode("utf-8"))
        log.debug("Encoded page %d/%d", i, len(pages))
    return encoded


def image_to_base64(image_path: Path) -> str:
    """Read an image file and return its base64-encoded string."""
    log.info("Encoding image: %s", image_path.name)
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def build_message_content(file_path: Path) -> list[dict]:
    """Build the content list for the Anthropic message (text + image blocks)."""
    ext = file_path.suffix.lower()
    content = [{"type": "text", "text": "Extract the insurance document data from the image(s) below."}]

    if ext == ".pdf":
        pages = pdf_to_base64_images(file_path)
        for i, b64 in enumerate(pages, start=1):
            log.info("Attaching page %d to request", i)
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/png", "data": b64},
            })
    else:
        mime = "image/jpeg" if ext in {".jpg", ".jpeg"} else "image/png"
        b64 = image_to_base64(file_path)
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": mime, "data": b64},
        })

    return content


def detect_cancellation_notice(*text_chunks: str) -> bool:
    """Return True when cancellation keywords are present in any provided text."""
    combined_text = " ".join(str(chunk or "") for chunk in text_chunks).lower()
    return any(keyword in combined_text for keyword in CANCELLATION_KEYWORDS)


def apply_simple_document_classification(data: dict, source_file: Path) -> dict:
    """Apply lightweight keyword-based document classification."""
    searchable = [
        source_file.name,
        data.get("document_type") or "",
        data.get("named_insured") or "",
        json.dumps(data, ensure_ascii=False),
    ]

    combined_text = " ".join(str(chunk or "") for chunk in searchable).lower()

    # Check reinstatement FIRST — reinstatement keywords are a subset of
    # CANCELLATION_KEYWORDS, so we must match the more specific type first.
    if any(kw in combined_text for kw in ("reinstatement", "reinstated", "policy reinstated", "notice of reinstatement")):
        data["document_type"] = "reinstatement"
        log.info("Keyword match found. document_type overridden to 'reinstatement'.")
    elif detect_cancellation_notice(*searchable):
        data["document_type"] = "cancellation_notice"
        log.info("Keyword match found. document_type overridden to 'cancellation_notice'.")

    return data


# ── Date normalization ───────────────────────────────────────────────────────

# Regex for 2-digit year: M/D/YY or MM/DD/YY (with / or - separators)
_SHORT_YEAR_RE = re.compile(
    r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{2})$"
)

# Ordered formats to try with strptime (most common COI formats first)
_DATE_FORMATS = (
    "%m/%d/%Y",       # 01/01/2025
    "%m-%d-%Y",       # 01-01-2025
    "%m/%d/%y",       # 01/01/25
    "%m-%d-%y",       # 01-01-25
    "%B %d, %Y",      # January 1, 2025
    "%B %d %Y",       # January 1 2025
    "%b %d, %Y",      # Jan 1, 2025
    "%b %d %Y",       # Jan 1 2025
    "%Y-%m-%d",       # 2025-01-01 (already ISO — pass through)
    "%d %B %Y",       # 01 January 2025
    "%d %b %Y",       # 01 Jan 2025
)


def _normalize_date(value: str) -> str:
    """Try to parse a date string and return ISO format (YYYY-MM-DD).

    Returns the original string unchanged if no format matches.
    """
    cleaned = value.strip()
    if not cleaned:
        return value

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return value


def normalize_policy_dates(data: dict) -> dict:
    """Normalize effective_date and expiration_date to ISO format across all policies."""
    date_fields = ("effective_date", "expiration_date")
    policies = data.get("policies") or []
    normalized_count = 0

    for policy in policies:
        for field in date_fields:
            raw = policy.get(field)
            if raw is None or not isinstance(raw, str) or raw.strip() == "":
                continue
            normalized = _normalize_date(raw)
            if normalized != raw:
                log.debug("Normalized %s: %r → %r", field, raw, normalized)
                policy[field] = normalized
                normalized_count += 1

    if normalized_count > 0:
        log.info("Normalized %d date field(s) to ISO format.", normalized_count)

    # Also normalize the top-level certificate_date if present
    cert_date = data.get("certificate_date")
    if cert_date and isinstance(cert_date, str) and cert_date.strip():
        normalized = _normalize_date(cert_date)
        if normalized != cert_date:
            log.debug("Normalized certificate_date: %r → %r", cert_date, normalized)
            data["certificate_date"] = normalized

    return data


def _is_blank(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _needs_second_pass_policy_extraction(data: dict) -> bool:
    policies = data.get("policies") or []
    if not policies:
        return False

    target_fields = ("policy_number", "effective_date", "expiration_date")
    total_cells = len(policies) * len(target_fields)
    blank_cells = 0

    for policy in policies:
        for field in target_fields:
            if _is_blank(policy.get(field)):
                blank_cells += 1

    return total_cells > 0 and (blank_cells / total_cells) >= 0.5


def _parse_json_response(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


def _run_second_pass_policy_extraction(client: anthropic.Anthropic, content: list[dict]) -> dict:
    response = client.messages.create(
        model="claude-opus-4-5",
        system=SECOND_PASS_POLICY_PROMPT,
        messages=[{"role": "user", "content": content}],
        max_tokens=1200,
        temperature=0,
    )
    raw = response.content[0].text.strip()
    log.debug("Raw second-pass Claude response:\n%s", raw)
    return _parse_json_response(raw)


def _run_dedicated_acord_policy_table_reader(client: anthropic.Anthropic, content: list[dict]) -> dict:
    response = client.messages.create(
        model="claude-opus-4-5",
        system=DEDICATED_ACORD_POLICY_TABLE_PROMPT,
        messages=[{"role": "user", "content": content}],
        max_tokens=1500,
        temperature=0,
    )
    raw = response.content[0].text.strip()
    log.debug("Raw dedicated policy-table response:\n%s", raw)
    return _parse_json_response(raw)


def _merge_second_pass_policy_fields(primary_data: dict, secondary_data: dict) -> str:
    primary_policies = primary_data.get("policies") or []
    secondary_policies = secondary_data.get("policies") or []
    recovered = []

    for idx, primary in enumerate(primary_policies):
        if idx >= len(secondary_policies):
            continue
        secondary = secondary_policies[idx] or {}

        for field in ("policy_number", "effective_date", "expiration_date"):
            if _is_blank(primary.get(field)) and not _is_blank(secondary.get(field)):
                primary[field] = secondary.get(field)
                recovered.append(f"row {idx + 1} {field}")

    return ", ".join(recovered) if recovered else "none"


def _should_run_dedicated_policy_table_reader(data: dict) -> bool:
    document_type = (data.get("document_type") or "").strip().lower()
    policies = data.get("policies") or []
    return document_type == "coi" and len(policies) > 0


def _merge_dedicated_policy_table_fields(primary_data: dict, secondary_data: dict) -> str:
    primary_policies = primary_data.get("policies") or []
    secondary_policies = secondary_data.get("policies") or []
    updated = []

    for idx, primary in enumerate(primary_policies):
        if idx >= len(secondary_policies):
            continue
        secondary = secondary_policies[idx] or {}

        for field in ("policy_type", "policy_number", "effective_date", "expiration_date", "coverage_limits"):
            if _is_blank(primary.get(field)) and not _is_blank(secondary.get(field)):
                primary[field] = secondary.get(field)
                updated.append(f"row {idx + 1} {field}")

    return ", ".join(updated) if updated else "none"


# ── Core extraction ───────────────────────────────────────────────────────────

def extract_document(file_path: Path) -> dict:
    """Send the document to Anthropic Claude and return parsed JSON."""
    if not ANTHROPIC_API_KEY:
        raise EnvironmentError("ANTHROPIC_API_KEY is not set in your .env file.")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    content = build_message_content(file_path)

    log.info("Sending request to Anthropic (model: claude-opus-4-5)…")
    response = client.messages.create(
        model="claude-opus-4-5",
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content}],
        max_tokens=2000,
        temperature=0,
    )

    raw = response.content[0].text.strip()
    log.debug("Raw Claude response:\n%s", raw)

    try:
        data = _parse_json_response(raw)
    except json.JSONDecodeError as exc:
        log.error("Claude returned non-JSON output:\n%s", raw)
        raise ValueError(f"Failed to parse Claude response as JSON: {exc}") from exc

    if _should_run_dedicated_policy_table_reader(data):
        log.info("Running dedicated ACORD policy-table reader")
        try:
            policy_table_data = _run_dedicated_acord_policy_table_reader(client, content)
            recovered_fields = _merge_dedicated_policy_table_fields(data, policy_table_data)
            log.info("Policy-table reader recovered: %s", recovered_fields)
        except json.JSONDecodeError as exc:
            log.warning("Dedicated policy-table reader returned non-JSON output: %s", exc)
        except Exception as exc:
            log.warning("Dedicated policy-table reader failed: %s", exc)

    return data


# ── Output ────────────────────────────────────────────────────────────────────

def save_extraction(data: dict, source_file: Path) -> Path:
    """Save the extracted JSON to extracted/<same_stem>.json."""
    EXTRACT_DIR.mkdir(exist_ok=True)
    out_path = EXTRACT_DIR / (source_file.stem + ".json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    log.info("Extraction saved → %s", out_path)
    return out_path


# ── Entry point ───────────────────────────────────────────────────────────────

def _process_single_file(file_path: Path) -> dict:
    """Extract, classify, normalize dates, and save results for a single file."""
    data = extract_document(file_path)
    data = normalize_policy_dates(data)
    data = apply_simple_document_classification(data, file_path)
    out_path = save_extraction(data, file_path)

    log.info("Extraction complete for %s", file_path.name)
    log.info("  document_type : %s", data.get("document_type"))
    log.info("  named_insured : %s", data.get("named_insured"))
    log.info("  policies found: %d", len(data.get("policies", [])))
    log.info("  output file   : %s", out_path)

    return data


def run():
    log.info("=== Module 2: Document Extractor started ===")

    try:
        # Single-file mode: CLI argument
        if len(sys.argv) > 1:
            file_path = Path(sys.argv[1])
            if not file_path.exists() or not file_path.is_file():
                raise FileNotFoundError(f"File not found: {file_path}")
            if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                raise FileNotFoundError(
                    f"Unsupported file type: {file_path.suffix}. Supported: {', '.join(SUPPORTED_EXTENSIONS)}"
                )
            log.info("Single-file mode: %s", file_path)
            _process_single_file(file_path)
            return

        # Batch mode: process all pending files in uploads/
        pending = get_all_pending_files(UPLOAD_DIR)
        if not pending:
            log.info("No pending files to extract. Done.")
            return

        log.info("Found %d pending file(s) to extract.", len(pending))
        succeeded = 0
        failed = 0

        for file_path in pending:
            log.info("--- Processing file %d/%d: %s ---",
                     succeeded + failed + 1, len(pending), file_path.name)
            try:
                _process_single_file(file_path)
                succeeded += 1
            except (ValueError, json.JSONDecodeError) as exc:
                log.error("Extraction failed for %s: %s", file_path.name, exc)
                failed += 1
            except Exception as exc:
                log.exception("Unexpected error processing %s: %s", file_path.name, exc)
                failed += 1

        log.info("=== Batch complete: %d succeeded, %d failed out of %d ===",
                 succeeded, failed, len(pending))

    except FileNotFoundError as exc:
        log.error("File error: %s", exc)
        sys.exit(1)
    except EnvironmentError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    run()
