"""Test that call_claude_with_retry retries on 500 and succeeds on second attempt."""

from unittest.mock import MagicMock, patch
import anthropic


def test_500_retries_then_succeeds():
    """First call raises 500, second call succeeds — should return the response."""
    from extractor import call_claude_with_retry

    mock_response = MagicMock()
    mock_response.content = [MagicMock(text='{"result": "ok"}')]

    error_500 = anthropic.APIStatusError(
        message="Internal Server Error",
        response=MagicMock(status_code=500, headers={}),
        body={"error": {"message": "Internal Server Error"}},
    )

    mock_create = MagicMock(side_effect=[error_500, mock_response])

    with patch("time.sleep"):  # Don't actually wait 10s in tests
        result = call_claude_with_retry(
            mock_create,
            model="claude-opus-4-5",
            messages=[{"role": "user", "content": "test"}],
            max_retries=3,
            retry_delay=10,
        )

    assert result is mock_response
    assert mock_create.call_count == 2
    print("Test passed: 500 error retried and succeeded on second attempt.")


if __name__ == "__main__":
    test_500_retries_then_succeeds()
