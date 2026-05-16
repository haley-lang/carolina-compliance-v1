"""Shared Cloudflare R2 / S3-compatible client helpers.

Used by airtable_backup.py (weekly CSV exports) and email_monitor.py
(per-intake PDF persistence). If any R2 env var is missing, get_r2_client()
returns (None, None) and callers skip upload silently — local-only fallback,
no crash.

Required env vars:
    R2_ACCESS_KEY_ID
    R2_SECRET_ACCESS_KEY
    R2_ACCOUNT_ID
    R2_BUCKET_NAME
"""

import logging
import mimetypes
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def get_r2_client():
    """Construct an S3-compatible boto3 client for Cloudflare R2.

    Returns a tuple (client, bucket_name) on success or (None, None) if any
    required env var is missing — caller treats None as "skip R2 upload,
    fall back to local-only behavior."
    """
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    account_id = os.getenv("R2_ACCOUNT_ID")
    bucket = os.getenv("R2_BUCKET_NAME")

    missing = [
        name for name, val in [
            ("R2_ACCESS_KEY_ID", access_key),
            ("R2_SECRET_ACCESS_KEY", secret_key),
            ("R2_ACCOUNT_ID", account_id),
            ("R2_BUCKET_NAME", bucket),
        ] if not val
    ]
    if missing:
        logger.warning(
            "R2 client unavailable — missing env var(s): %s. "
            "R2-dependent operations will be skipped (local-only fallback).",
            ", ".join(missing),
        )
        return None, None

    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ImportError as exc:
        logger.error("R2 client unavailable — boto3 not installed: %s", exc)
        return None, None

    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(signature_version="s3v4"),
        region_name="auto",
    )
    return client, bucket


def upload_file_to_r2(client, bucket, local_path, key, content_type=None) -> bool:
    """Upload one local file to R2. Returns True on success, False on failure.

    Failure is logged but never raises — caller continues with the next file
    or with downstream work (intake completion, etc.).

    If content_type is None, infers from the file extension via mimetypes
    (e.g. .pdf -> application/pdf, .png -> image/png). Falls back to
    application/octet-stream if mimetypes can't guess.
    """
    local_path = Path(local_path)
    if content_type is None:
        guessed, _ = mimetypes.guess_type(local_path.name)
        content_type = guessed or "application/octet-stream"

    try:
        client.upload_file(
            Filename=str(local_path),
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ContentType": content_type},
        )
        return True
    except Exception as exc:
        logger.error(
            "R2 upload failed for %s -> s3://%s/%s: %s",
            local_path.name, bucket, key, exc,
        )
        return False
