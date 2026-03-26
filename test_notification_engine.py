# Test script for NotificationEngine

from notification_rule import NotificationRule
from notification_event import NotificationEvent
from notification_engine import NotificationEngine
from unittest.mock import Mock

# Mock SendGrid service
mock_sendgrid_service = Mock()

# Sample rules
rules = [
    NotificationRule(id=1, name="License Expiring", event_type="LICENSE_EXPIRING", condition_json=[{"type": "equals", "key": "days_left", "value": 5}], template_id="template_1", enabled=True),
    NotificationRule(id=2, name="Status Changed", event_type="STATUS_CHANGED", condition_json=[{"type": "exists", "key": "new_status"}], template_id="template_2", enabled=True)
]

# Initialize NotificationEngine
engine = NotificationEngine(rules, mock_sendgrid_service)

# Sample events
event1 = NotificationEvent(event_type="LICENSE_EXPIRING", payload={"days_left": 5}, timestamp="2026-03-18T20:00:00Z")
event2 = NotificationEvent(event_type="STATUS_CHANGED", payload={"new_status": "active"}, timestamp="2026-03-18T20:05:00Z")

# Process events
engine.processEvent(event1)
engine.processEvent(event2)

# Verify that emails were sent
mock_sendgrid_service.send_email.assert_any_call("template_1", event1.payload)
mock_sendgrid_service.send_email.assert_any_call("template_2", event2.payload)