import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
from compliance_deadline import ComplianceDeadline
from deadline_scheduler import DeadlineScheduler

class TestDeadlineScheduler(unittest.TestCase):
    def setUp(self):
        self.session = MagicMock()
        mock_rules = MagicMock()
        mock_sendgrid_service = MagicMock()
        self.scheduler = DeadlineScheduler(self.session, mock_rules, mock_sendgrid_service)

    def test_run(self):
        # Setup mock deadlines
        pending_deadline = ComplianceDeadline(
            id=3,
            entity_id=103,
            entity_type='TypeC',
            deadline_type='Type3',
            due_date=datetime.now() + timedelta(days=1),
            status='PENDING'
        )
        overdue_deadline = ComplianceDeadline(
            id=4,
            entity_id=104,
            entity_type='TypeD',
            deadline_type='Type4',
            due_date=datetime.now() - timedelta(days=1),
            status='PENDING'
        )
        self.session.query.return_value.filter.return_value.all.return_value = [pending_deadline, overdue_deadline]

        # Run scheduler
        self.scheduler.run()

        # Verify overdue deadline is marked and notification emitted
        self.assertEqual(overdue_deadline.status, 'OVERDUE')
        self.assertIsNotNone(overdue_deadline.last_event_sent_at)

        # Verify upcoming deadline notification emitted
        self.assertIsNotNone(pending_deadline.last_event_sent_at)

        # Verify session commit called twice (once for each deadline)
        self.assertEqual(self.session.commit.call_count, 2)

        deadline = ComplianceDeadline(
            id=1,
            entity_id=101,
            entity_type='TypeA',
            deadline_type='Type1',
            due_date=datetime.now() - timedelta(days=1),
            status='PENDING'
        )
        self.scheduler.mark_overdue(deadline)
        self.assertEqual(deadline.status, 'OVERDUE')
        self.session.commit.assert_called_once()

    def test_emit_notification(self):
        deadline = ComplianceDeadline(
            id=2,
            entity_id=102,
            entity_type='TypeB',
            deadline_type='Type2',
            due_date=datetime.now() + timedelta(days=2),
            status='PENDING'
        )
        self.scheduler.emit_notification(deadline, "DEADLINE_UPCOMING")
        self.assertIsNotNone(deadline.last_event_sent_at)
        self.session.commit.assert_called_once()

if __name__ == '__main__':
    unittest.main()