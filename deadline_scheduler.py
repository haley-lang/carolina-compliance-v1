import logging
from datetime import datetime, timedelta
from compliance_deadline import ComplianceDeadline
from notification_engine import NotificationEngine

class DeadlineScheduler:
    def __init__(self, session, rules, sendgrid_service, threshold_days=3):
        self.notification_engine = NotificationEngine(rules, sendgrid_service)
        self.session = session
        self.threshold_days = threshold_days
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def run(self):
        logging.info("Scheduler job started. Scanning for pending deadlines.")
        today = datetime.now()
        upcoming_threshold = today + timedelta(days=self.threshold_days)

        # Load active PENDING deadlines
        pending_deadlines = self.session.query(ComplianceDeadline).filter(
            ComplianceDeadline.status == 'PENDING'
        ).all()

        logging.info(f"Loaded {len(pending_deadlines)} pending deadlines for processing.")
        for deadline in pending_deadlines:
            if deadline.due_date <= today:
                self.mark_overdue(deadline)
            elif deadline.due_date <= upcoming_threshold:
                logging.info(f"Upcoming deadline detected for deadline ID {deadline.id}. Emitting DEADLINE_UPCOMING event.")
                self.emit_notification(deadline, "DEADLINE_UPCOMING")

    def mark_overdue(self, deadline):
        if deadline.status == 'PENDING':
            deadline.status = 'OVERDUE'
            self.session.commit()
            logging.info(f"Marked deadline ID {deadline.id} as OVERDUE. Emitting DEADLINE_OVERDUE event.")
            self.emit_notification(deadline, "DEADLINE_OVERDUE")

            # Check for escalation
            rule = next((rule for rule in self.notification_engine.rules if rule.event_type == "DEADLINE_OVERDUE"), None)
            if rule and rule.escalation_delay_days is not None:
                overdue_days = (datetime.now() - deadline.due_date).days
                if overdue_days > rule.escalation_delay_days:
                    logging.info(f"Deadline ID {deadline.id} has been overdue for {overdue_days} days. Emitting DEADLINE_ESCALATED event.")
                    self.emit_notification(deadline, "DEADLINE_ESCALATED")

    def emit_notification(self, deadline, event_type):
        if not deadline.last_event_sent_at or deadline.last_event_sent_at < datetime.now() - timedelta(days=1):
            payload = {
                "entity_id": deadline.entity_id,
                "entity_type": deadline.entity_type,
                "deadline_type": deadline.deadline_type,
                "due_date": deadline.due_date,
                "days_remaining": (deadline.due_date - datetime.now()).days
            }
            event = {
                "type": event_type,
                "payload": payload
            }
            self.notification_engine.processEvent(event)
            deadline.last_event_sent_at = datetime.now()
            self.session.commit()
            logging.info(f"Emitted {event_type} event for deadline ID {deadline.id}.")
