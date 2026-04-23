"""Test that expiration reminders are not duplicated at the same threshold."""

from module_12_vendor_reminder_engine import policy_triggers_reminder


def test_same_threshold_does_not_trigger():
    """A policy at 'Expiring in 30 Days' with Last Reminder Threshold='30'
    should NOT trigger a reminder — it was already sent at this level."""
    fields = {
        "Expiration Status": "Expiring in 30 Days",
        "Last Reminder Threshold": "30",
    }
    assert policy_triggers_reminder(fields) is False


def test_new_threshold_triggers():
    """A policy at 'Expiring in 7 Days' with Last Reminder Threshold='30'
    SHOULD trigger — it crossed into a new threshold."""
    fields = {
        "Expiration Status": "Expiring in 7 Days",
        "Last Reminder Threshold": "30",
    }
    assert policy_triggers_reminder(fields) is True


def test_no_threshold_yet_triggers():
    """A policy at 'Expiring in 30 Days' with no Last Reminder Threshold
    SHOULD trigger — first time at any threshold."""
    fields = {
        "Expiration Status": "Expiring in 30 Days",
        "Last Reminder Threshold": "",
    }
    assert policy_triggers_reminder(fields) is True


def test_expired_after_7_triggers():
    """A policy that moved from '7' to 'Expired' SHOULD trigger."""
    fields = {
        "Expiration Status": "Expired",
        "Last Reminder Threshold": "7",
    }
    assert policy_triggers_reminder(fields) is True


def test_active_policy_does_not_trigger():
    """An 'Active' policy should never trigger a reminder."""
    fields = {
        "Expiration Status": "Active",
        "Last Reminder Threshold": "",
    }
    assert policy_triggers_reminder(fields) is False


def test_expired_already_sent_does_not_trigger():
    """An 'Expired' policy with threshold='0' should NOT re-trigger."""
    fields = {
        "Expiration Status": "Expired",
        "Last Reminder Threshold": "0",
    }
    assert policy_triggers_reminder(fields) is False


if __name__ == "__main__":
    test_same_threshold_does_not_trigger()
    test_new_threshold_triggers()
    test_no_threshold_yet_triggers()
    test_expired_after_7_triggers()
    test_active_policy_does_not_trigger()
    test_expired_already_sent_does_not_trigger()
    print("All 6 tests passed.")
