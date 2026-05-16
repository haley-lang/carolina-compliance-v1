"""Unit tests for r2_storage.get_r2_client and r2_storage.upload_file_to_r2."""
import os
from unittest.mock import MagicMock, patch

import pytest

import r2_storage


R2_ENV_VARS = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ACCOUNT_ID", "R2_BUCKET_NAME")


@pytest.fixture
def clean_r2_env(monkeypatch):
    """Strip all R2_* env vars so get_r2_client() sees them as missing."""
    for v in R2_ENV_VARS:
        monkeypatch.delenv(v, raising=False)


@pytest.fixture
def populated_r2_env(monkeypatch):
    """Set R2_* env vars to test values."""
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-key-id")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("R2_ACCOUNT_ID", "test-account")
    monkeypatch.setenv("R2_BUCKET_NAME", "test-bucket")


def test_get_r2_client_returns_none_when_env_missing(clean_r2_env):
    client, bucket = r2_storage.get_r2_client()
    assert client is None
    assert bucket is None


def test_get_r2_client_builds_boto3_client_when_env_present(populated_r2_env):
    fake_client = MagicMock(name="boto3_client")
    with patch("boto3.client", return_value=fake_client) as mock_boto:
        client, bucket = r2_storage.get_r2_client()
    assert client is fake_client
    assert bucket == "test-bucket"
    # Verify endpoint and credentials passed correctly
    call_kwargs = mock_boto.call_args.kwargs
    assert call_kwargs["endpoint_url"] == "https://test-account.r2.cloudflarestorage.com"
    assert call_kwargs["aws_access_key_id"] == "test-key-id"
    assert call_kwargs["aws_secret_access_key"] == "test-secret"
    assert call_kwargs["region_name"] == "auto"


def test_upload_file_to_r2_success(tmp_path):
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")

    fake_client = MagicMock()
    ok = r2_storage.upload_file_to_r2(fake_client, "bucket", pdf_path, "key/test.pdf")

    assert ok is True
    fake_client.upload_file.assert_called_once()
    call_kwargs = fake_client.upload_file.call_args.kwargs
    assert call_kwargs["Filename"] == str(pdf_path)
    assert call_kwargs["Bucket"] == "bucket"
    assert call_kwargs["Key"] == "key/test.pdf"
    assert call_kwargs["ExtraArgs"] == {"ContentType": "application/pdf"}


def test_upload_file_to_r2_failure_returns_false(tmp_path):
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(b"x")

    fake_client = MagicMock()
    fake_client.upload_file.side_effect = RuntimeError("R2 network error")

    ok = r2_storage.upload_file_to_r2(fake_client, "bucket", pdf_path, "key/test.pdf")
    assert ok is False  # Failure logged, no raise


def test_upload_file_to_r2_infers_content_type_from_extension(tmp_path):
    fake_client = MagicMock()
    cases = [
        ("doc.pdf", "application/pdf"),
        ("img.png", "image/png"),
        ("img.jpg", "image/jpeg"),
        ("img.jpeg", "image/jpeg"),
    ]
    for fname, expected_ct in cases:
        p = tmp_path / fname
        p.write_bytes(b"x")
        r2_storage.upload_file_to_r2(fake_client, "b", p, f"k/{fname}")
        actual_ct = fake_client.upload_file.call_args.kwargs["ExtraArgs"]["ContentType"]
        assert actual_ct == expected_ct, f"{fname}: expected {expected_ct}, got {actual_ct}"
