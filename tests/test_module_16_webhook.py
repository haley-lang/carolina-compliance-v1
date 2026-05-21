"""Unit tests for module_16_inbound_webhook subscription lifecycle handlers.

Each handler is exercised with a canned Stripe event payload (a simple dict
shaped like Stripe's webhook payload). Airtable + SendGrid are mocked.

Coverage:
  - _resolve_tier (price-id mapping + amount fallback + unresolved)
  - _ts_to_date_str (Stripe Unix timestamp -> ISO date)
  - _extract_coupon_code (skipped — requires stripe.checkout.Session.retrieve mocking; covered by integration)
  - _handle_subscription_deleted (Client found → status + ended date + email; not found → log only)
  - _handle_invoke_payment_failed (Client found → Past Due; not found → log)
  - _handle_invoice_paid (Past Due → Active; other status → no-op; not found → ignore)
  - _handle_subscription_updated (cancel_at_period_end log; tier change write; coupon-expired Trial→Active)

The existing checkout.session.completed end-to-end flow is NOT re-tested here;
it relies on live Stripe + Airtable + SendGrid. Manual test via Stripe CLI
per the docstring at the top of module_16_inbound_webhook.py.
"""
from unittest.mock import MagicMock, patch

import pytest


# ── Unit tests: pure helpers ─────────────────────────────────────────────────


def test_resolve_tier_by_price_id_starter():
    from module_16_inbound_webhook import _resolve_tier
    assert _resolve_tier(price_id="price_starter") == "Starter"


def test_resolve_tier_by_price_id_growth():
    from module_16_inbound_webhook import _resolve_tier
    assert _resolve_tier(price_id="price_growth") == "Growth"


def test_resolve_tier_by_price_id_scale():
    from module_16_inbound_webhook import _resolve_tier
    assert _resolve_tier(price_id="price_scale") == "Scale"


def test_resolve_tier_by_amount_fallback():
    from module_16_inbound_webhook import _resolve_tier
    assert _resolve_tier(price_id="", amount_cents=14900) == "Starter"
    assert _resolve_tier(price_id="", amount_cents=39900) == "Growth"
    assert _resolve_tier(price_id="", amount_cents=79900) == "Scale"


def test_resolve_tier_unresolved_returns_none():
    from module_16_inbound_webhook import _resolve_tier
    assert _resolve_tier(price_id="unknown", amount_cents=99999) is None
    assert _resolve_tier(price_id="", amount_cents=None) is None


def test_ts_to_date_str_converts_stripe_timestamp():
    from datetime import datetime, timezone
    from module_16_inbound_webhook import _ts_to_date_str
    # Build the timestamp from a known datetime to avoid hand-math errors.
    ts = int(datetime(2026, 5, 20, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    assert _ts_to_date_str(ts) == "2026-05-20"


def test_ts_to_date_str_handles_none():
    from module_16_inbound_webhook import _ts_to_date_str
    assert _ts_to_date_str(None) == ""
    assert _ts_to_date_str(0) == ""


def test_ts_to_date_str_handles_garbage():
    from module_16_inbound_webhook import _ts_to_date_str
    assert _ts_to_date_str("not_a_number") == ""


# ── Handler tests: customer.subscription.deleted ─────────────────────────────


def _make_event(event_type: str, data_object: dict, previous_attributes: dict = None) -> dict:
    """Construct a minimal Stripe event dict shaped like a webhook payload."""
    event = {
        "type": event_type,
        "data": {"object": data_object},
    }
    if previous_attributes is not None:
        event["data"]["previous_attributes"] = previous_attributes
    return event


@patch("module_16_inbound_webhook._send_cancellation_email")
@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_subscription_id")
def test_handle_subscription_deleted_marks_canceled_and_sends_email(
    mock_find, mock_table, mock_send_cancel,
):
    from module_16_inbound_webhook import _handle_subscription_deleted

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Acme Builders",
            "Primary Contact Email": "acme@example.com",
        },
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    from datetime import datetime, timezone
    canceled_ts = int(datetime(2026, 5, 20, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    event = _make_event("customer.subscription.deleted", {
        "id": "sub_123",
        "canceled_at": canceled_ts,
        "ended_at": None,
    })

    _handle_subscription_deleted(event)

    # Airtable update fires with status + ended date
    mock_table_instance.update.assert_called_once()
    call_args = mock_table_instance.update.call_args
    assert call_args.args[0] == "recCLIENT1"
    update_fields = call_args.args[1]
    assert update_fields["fld4ZGRZ71BWabXwC"] == "Canceled"  # Subscription Status
    assert update_fields["fld9SrkYleqvkUaHO"] == "2026-05-20"  # Subscription Ended
    assert call_args.kwargs.get("typecast") is True

    # Cancellation email fires with the client's contact info
    mock_send_cancel.assert_called_once_with("Acme Builders", "acme@example.com")


@patch("module_16_inbound_webhook._send_cancellation_email")
@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_subscription_id")
def test_handle_subscription_deleted_no_client_logs_and_returns(
    mock_find, mock_table, mock_send_cancel,
):
    from module_16_inbound_webhook import _handle_subscription_deleted
    mock_find.return_value = None

    event = _make_event("customer.subscription.deleted", {"id": "sub_orphan"})
    _handle_subscription_deleted(event)

    mock_table.assert_not_called()
    mock_send_cancel.assert_not_called()


# ── Handler tests: invoice.payment_failed ────────────────────────────────────


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_customer_id")
def test_handle_invoice_payment_failed_marks_past_due(mock_find, mock_table):
    from module_16_inbound_webhook import _handle_invoice_payment_failed

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {"Client Name": "Acme Builders"},
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    event = _make_event("invoice.payment_failed", {"customer": "cus_123"})
    _handle_invoice_payment_failed(event)

    mock_table_instance.update.assert_called_once()
    call_args = mock_table_instance.update.call_args
    assert call_args.args[0] == "recCLIENT1"
    assert call_args.args[1]["fld4ZGRZ71BWabXwC"] == "Past Due"
    assert call_args.kwargs.get("typecast") is True


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_customer_id")
def test_handle_invoice_payment_failed_no_client_no_op(mock_find, mock_table):
    from module_16_inbound_webhook import _handle_invoice_payment_failed
    mock_find.return_value = None

    event = _make_event("invoice.payment_failed", {"customer": "cus_unknown"})
    _handle_invoice_payment_failed(event)

    mock_table.assert_not_called()


# ── Handler tests: invoice.paid ──────────────────────────────────────────────


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_customer_id")
def test_handle_invoice_paid_flips_past_due_to_active(mock_find, mock_table):
    from module_16_inbound_webhook import _handle_invoice_paid

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Acme Builders",
            "Subscription Status": "Past Due",
        },
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    event = _make_event("invoice.paid", {"customer": "cus_123"})
    _handle_invoice_paid(event)

    mock_table_instance.update.assert_called_once()
    call_args = mock_table_instance.update.call_args
    assert call_args.args[1]["fld4ZGRZ71BWabXwC"] == "Active"


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_customer_id")
def test_handle_invoice_paid_no_op_when_status_is_active(mock_find, mock_table):
    from module_16_inbound_webhook import _handle_invoice_paid

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Acme Builders",
            "Subscription Status": "Active",
        },
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    event = _make_event("invoice.paid", {"customer": "cus_123"})
    _handle_invoice_paid(event)

    mock_table_instance.update.assert_not_called()


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_customer_id")
def test_handle_invoice_paid_no_client_no_op(mock_find, mock_table):
    from module_16_inbound_webhook import _handle_invoice_paid
    mock_find.return_value = None

    event = _make_event("invoice.paid", {"customer": "cus_orphan"})
    _handle_invoice_paid(event)

    mock_table.assert_not_called()


# ── Handler tests: customer.subscription.updated ─────────────────────────────


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_subscription_id")
def test_handle_subscription_updated_cancel_at_period_end_log_only(mock_find, mock_table):
    """cancel_at_period_end False -> True: log only, no Airtable write."""
    from module_16_inbound_webhook import _handle_subscription_updated

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Acme Builders",
            "Tier": "Starter",
            "Subscription Status": "Active",
        },
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    event = _make_event(
        "customer.subscription.updated",
        {
            "id": "sub_123",
            "cancel_at_period_end": True,
            # No tier change, no discount removal — only cancel flag flipped
            "items": {"data": [{"price": {"id": "price_starter"}}]},
            "discount": None,
        },
        previous_attributes={"cancel_at_period_end": False},
    )
    _handle_subscription_updated(event)

    # No Airtable updates fire — current tier matches, no coupon to expire
    mock_table_instance.update.assert_not_called()


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_subscription_id")
def test_handle_subscription_updated_tier_change_writes_both_fields(mock_find, mock_table):
    """Tier change: write Tier + Subscription Tier in sync."""
    from module_16_inbound_webhook import _handle_subscription_updated

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Acme Builders",
            "Tier": "Starter",
            "Subscription Status": "Active",
        },
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    event = _make_event(
        "customer.subscription.updated",
        {
            "id": "sub_123",
            "items": {"data": [{"price": {"id": "price_growth"}}]},
            "discount": None,
        },
    )
    _handle_subscription_updated(event)

    mock_table_instance.update.assert_called_once()
    call_args = mock_table_instance.update.call_args
    update_fields = call_args.args[1]
    assert update_fields["fld57t54w0FMqN32C"] == "Growth"  # Tier
    assert update_fields["fldXnhsv3ntup5Gpy"] == "Growth"  # Subscription Tier (in sync)
    assert call_args.kwargs.get("typecast") is True


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_subscription_id")
def test_handle_subscription_updated_coupon_expired_flips_trial_to_active(mock_find, mock_table):
    """previous_attributes.discount was set, now object.discount is None,
    current Subscription Status is Trial → flip to Active."""
    from module_16_inbound_webhook import _handle_subscription_updated

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Acme Builders",
            "Tier": "Starter",
            "Subscription Status": "Trial",
        },
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    event = _make_event(
        "customer.subscription.updated",
        {
            "id": "sub_123",
            "items": {"data": [{"price": {"id": "price_starter"}}]},
            "discount": None,  # discount removed
        },
        previous_attributes={"discount": {"coupon": {"id": "FIRST49"}}},
    )
    _handle_subscription_updated(event)

    mock_table_instance.update.assert_called_once()
    call_args = mock_table_instance.update.call_args
    assert call_args.args[1]["fld4ZGRZ71BWabXwC"] == "Active"
    assert call_args.kwargs.get("typecast") is True


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_subscription_id")
def test_handle_subscription_updated_coupon_expired_but_status_not_trial_no_flip(mock_find, mock_table):
    """If status is already Active (not Trial), don't fire a redundant write."""
    from module_16_inbound_webhook import _handle_subscription_updated

    mock_find.return_value = {
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Acme Builders",
            "Tier": "Starter",
            "Subscription Status": "Active",
        },
    }
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    event = _make_event(
        "customer.subscription.updated",
        {
            "id": "sub_123",
            "items": {"data": [{"price": {"id": "price_starter"}}]},
            "discount": None,
        },
        previous_attributes={"discount": {"coupon": {"id": "FIRST49"}}},
    )
    _handle_subscription_updated(event)

    # No writes — tier matches, status is already Active
    mock_table_instance.update.assert_not_called()


@patch("module_16_inbound_webhook._clients_table")
@patch("module_16_inbound_webhook._find_client_by_stripe_subscription_id")
def test_handle_subscription_updated_no_client_no_op(mock_find, mock_table):
    from module_16_inbound_webhook import _handle_subscription_updated
    mock_find.return_value = None

    event = _make_event("customer.subscription.updated", {"id": "sub_orphan"})
    _handle_subscription_updated(event)

    mock_table.assert_not_called()
