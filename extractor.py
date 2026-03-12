"""
Module 2 — COI Document Extractor
Reads the newest file from the uploads/ folder, sends it to OpenAI GPT-4o
for structured data extraction, and saves the result as JSON in extracted/.
"""

import base64
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "uploads"))
EXTRACT_DIR = Path("extracted")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUPPORTED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png"}

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Extraction prompt ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a document analysis assistant specializing in insurance documents.
Extract structured data from the provided document image(s) and return ONLY valid JSON — no prose, no markdown fences.

Rules:
- Be conservative. Only extract data that is clearly visible in the document.
- If a field cannot be found, return null (for scalar fields) or [] (for arrays).
- Do not infer, guess, or fabricate policy numbers, dates, or coverage limits.
- If the document type is ambiguous or unrecognizable, set document_type to "unknown".
- For contact_emails, include any email addresses visible anywhere in the document.
- coverage_limits should be a plain string describing the limits as shown (e.g. "$1,000,000 / $2,000,000").

Return this exact JSON structure:
{
  "document_type": "<COI | cancellation_notice | endorsement | unknown>",
  "named_insured": "<string or null>",
  "certificate_date": "<string or null>",
  "contact_emails": ["<email>"],
  "policies": [
    {
      "policy_type": "<string or null>",
      "policy_number": "<string or null>",
      "carrier": "<string or null>",
      "effective_date": "<string or null>",
      "expiration_date": "<string or null>",
      "coverage_limits": "<string or null>"
    }
  ]
}"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_newest_file(folder: Path) -> Path:
    """Return the most recently modified supported file in the folder."""
    if not folder.exists():
        raise FileNotFoundError(f"Upload folder not found: {folder}")

    candidates = [
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    if not candidates:
        raise FileNotFoundError(
            f"No supported files ({', '.join(SUPPORTED_EXTENSIONS)}) found in {folder}"
        )

    newest = max(candidates, key=lambda f: f.stat().st_mtime)
    log.info("Selected file: %s (modified %s)", newest.name,
             datetime.fromtimestamp(newest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"))
    return newest


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
    """Build the content list for the OpenAI message (text + image blocks)."""
    ext = file_path.suffix.lower()
    content = [{"type": "text", "text": "Extract the insurance document data from the image(s) below."}]

    if ext == ".pdf":
        pages = pdf_to_base64_images(file_path)
        for i, b64 in enumerate(pages, start=1):
            log.info("Attaching page %d to request", i)
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "high"},
            })
    else:
        mime = "image/jpeg" if ext in {".jpg", ".jpeg"} else "image/png"
        b64 = image_to_base64(file_path)
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:{mime};base64,{b64}", "detail": "high"},
        })

    return content


# ── Core extraction ───────────────────────────────────────────────────────────

def extract_document(file_path: Path) -> dict:
    """Send the document to OpenAI and return parsed JSON."""
    if not OPENAI_API_KEY:
        raise EnvironmentError("OPENAI_API_KEY is not set in your .env file.")

    client = OpenAI(api_key=OPENAI_API_KEY)
    content = build_message_content(file_path)

    log.info("Sending request to OpenAI (model: gpt-4o)…")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
        max_tokens=2000,
        temperature=0,
    )

    raw = response.choices[0].message.content.strip()
    log.debug("Raw OpenAI response:\n%s", raw)

    # Strip markdown fences if the model included them anyway
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.error("OpenAI returned non-JSON output:\n%s", raw)
        raise ValueError(f"Failed to parse OpenAI response as JSON: {exc}") from exc

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

def run():
    log.info("=== Module 2: Document Extractor started ===")

    try:
        file_path = get_newest_file(UPLOAD_DIR)
        data = extract_document(file_path)
        out_path = save_extraction(data, file_path)

        log.info("Extraction complete.")
        log.info("document_type : %s", data.get("document_type"))
        log.info("named_insured : %s", data.get("named_insured"))
        log.info("policies found: %d", len(data.get("policies", [])))
        log.info("Output file   : %s", out_path)

        return data

    except FileNotFoundError as exc:
        log.error("File error: %s", exc)
        sys.exit(1)
    except EnvironmentError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)
    except ValueError as exc:
        log.error("Extraction error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("Unexpected error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    run()
