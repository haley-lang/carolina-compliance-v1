# NotificationEngine service

from condition_evaluator import ConditionEvaluator
from notification_audit_log import NotificationAuditLog
from email_notification_channel import EmailNotificationChannel
from sqlalchemy import create_engine
from notification_digest_item import NotificationDigestItem
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from pytz import timezone
from notification_template import NotificationTemplate
from compliance_deadline import ComplianceDeadline

class NotificationEngine:
    def __init__(self, rules, sendgrid_service):
        self.rules = rules
        self.channel = EmailNotificationChannel(sendgrid_service)  # Default channel
        self.sendgrid_service = sendgrid_service

    def processEvent(self, event):
        # Set up database session
        engine = create_engine('sqlite:///notifications.db')  # Example connection string
        Session = sessionmaker(bind=engine)
        session = Session()
        # Check notification preferences
        preference = session.query(NotificationPreference).filter_by(
            entity_id=event.payload.get('entity_id'),
            entity_type=event.payload.get('entity_type'),
            event_type=event.type,
            channel='email'  # Assuming email channel for this example
        ).first()

        # Check for quiet hours
        if preference and preference.quiet_hours_start and preference.quiet_hours_end and preference.timezone:
            current_time = datetime.now(timezone(preference.timezone))
            if preference.quiet_hours_start <= current_time.time() <= preference.quiet_hours_end:
                # Queue as digest or pending delivery
                digest_item = NotificationDigestItem(
                    recipient=event.payload.get('recipient'),
                    event_type=event.type,
                    payload_json=event.payload,
                    template_id=None  # Assuming template_id is not needed for digest
                )
                session.add(digest_item)
                session.commit()
                # Log DEFERRED_QUIET_HOURS
                audit_log = NotificationAuditLog(
                    rule_id=None,  # No specific rule matched
                    event_type=event.type,
                    recipient=event.payload.get('recipient'),
                    template_id=None,  # No template used
                    status='DEFERRED_QUIET_HOURS'
                )
                session.add(audit_log)
                session.commit()
                return

        if preference and preference.digest_mode == "DAILY":
            # Store in NotificationDigestItem instead of sending immediately
            digest_item = NotificationDigestItem(
                recipient=event.payload.get('recipient'),
                event_type=event.type,
                payload_json=event.payload,
                template_id=None  # Assuming template_id is not needed for digest
            )
            session.add(digest_item)
            session.commit()
            # Log QUEUED_DIGEST
            audit_log = NotificationAuditLog(
                rule_id=None,  # No specific rule matched
                event_type=event.type,
                recipient=event.payload.get('recipient'),
                template_id=None,  # No template used
                status='QUEUED_DIGEST'
            )
            session.add(audit_log)
            session.commit()
            return

        if preference and not preference.enabled:
            # Log SKIPPED_DISABLED
            audit_log = NotificationAuditLog(
                rule_id=None,  # No specific rule matched
                event_type=event.type,
                recipient=event.payload.get('recipient'),
                template_id=None,  # No template used
                status='SKIPPED_DISABLED'
            )
            session.add(audit_log)
            session.commit()
            return

        matching_rules = [rule for rule in self.rules if rule.enabled and rule.event_type == event.type]

        # Evaluate condition_json against event.payload
        for rule in matching_rules:
            if self.evaluate_conditions(rule.condition_json, event.payload):
                # Validate required variables
                missing_variables = [var for var in rule.required_variables if var not in event.payload]
                if missing_variables:
                    # Log SKIPPED_INVALID_PAYLOAD
                    audit_log = NotificationAuditLog(
                        rule_id=rule.id,
                        event_type=event.type,
                        recipient=event.payload.get('recipient'),
                        template_id=rule.template_id,
                        status='SKIPPED_INVALID_PAYLOAD',
                        error_message=f"Missing variables: {', '.join(missing_variables)}"
                    )
                    session.add(audit_log)
                    session.commit()
                    continue

                recipient = event.payload.get('recipient')
                if event.type == "DEADLINE_ESCALATED" and rule.escalation_recipient:
                    recipient = rule.escalation_recipient

                # Check for existing SUCCESS record
                existing_log = session.query(NotificationAuditLog).filter_by(
                    rule_id=rule.id,
                    event_type=event.type,
                    recipient=recipient,
                    template_id=rule.template_id,
                    status='SUCCESS'
                ).first()

                if existing_log:
                    # Skip send and log SKIPPED_DUPLICATE
                    audit_log = NotificationAuditLog(
                        rule_id=rule.id,
                        event_type=event.type,
                        recipient=event.payload.get('recipient'),
                        template_id=rule.template_id,
                        status='SKIPPED_DUPLICATE'
                    )
                    session.add(audit_log)
                    session.commit()
                    continue
                # Call existing SendGrid service
                try:
                    # Update payload with determined recipient
                    event.payload['recipient'] = recipient
                    # Use channel abstraction to send notification
                    self.channel.send(recipient, rule.template_id, event.payload)
                    status = 'SUCCESS'
                    error_message = None
                except Exception as e:
                    status = 'FAILED'
                    retry_count = 1
                    next_retry_at = datetime.utcnow() + timedelta(minutes=5)  # Simple backoff example
                    error_message = str(e)
                    audit_log.retry_count = retry_count
                    audit_log.next_retry_at = next_retry_at

                # Log rule matched and email sent
                self.log_event(rule, event)

                # Log to NotificationAuditLog
                audit_log = NotificationAuditLog(
                    rule_id=rule.id,
                    event_type=event.type,
                    recipient=event.payload.get('recipient'),
                    template_id=rule.template_id,
                    status=status,
                    error_message=error_message
                )
                session.add(audit_log)
                session.commit()

    def run_weekly_summary(self):
        # Set up database session
        engine = create_engine('sqlite:///notifications.db')  # Example connection string
        Session = sessionmaker(bind=engine)
        session = Session()

        # Group by organization and summarize deadlines
        organizations = session.query(NotificationTemplate.organization_id).distinct()
        for org in organizations:
            upcoming_deadlines = session.query(ComplianceDeadline).filter(
                ComplianceDeadline.organization_id == org,
                ComplianceDeadline.due_date > datetime.now()
            ).count()

            overdue_deadlines = session.query(ComplianceDeadline).filter(
                ComplianceDeadline.organization_id == org,
                ComplianceDeadline.due_date <= datetime.now()
            ).count()

            failed_notifications = session.query(NotificationAuditLog).filter(
                NotificationAuditLog.organization_id == org,
                NotificationAuditLog.status == 'FAILED'
            ).count()

            # Create email content
            template = session.query(NotificationTemplate).filter_by(name='Weekly Summary').first()
            if not template:
                continue

            subject = template.subject
            body = template.body.format(
                upcoming_deadlines=upcoming_deadlines,
                overdue_deadlines=overdue_deadlines,
                failed_notifications=failed_notifications
            )

            # Send email
            try:
                self.channel.send('admin@example.com', template.id, {'subject': subject, 'body': body})
                status = 'SUCCESS'
            except Exception as e:
                status = 'FAILED'
                error_message = str(e)

            # Log audit
            audit_log = NotificationAuditLog(
                rule_id=None,
                event_type='WEEKLY_SUMMARY',
                recipient='admin@example.com',
                template_id=template.id,
                status=status,
                organization_id=org,
                error_message=error_message if status == 'FAILED' else None
            )
            session.add(audit_log)
            session.commit()

    def evaluate_conditions(self, condition_json, payload):
        # Evaluate conditions using ConditionEvaluator
        return all(ConditionEvaluator.evaluate(cond, payload) for cond in condition_json)

    def log_event(self, rule, event):
        print(f"Rule {rule.name} matched. Email sent using template {rule.template_id}. Event: {event.type}, Payload: {event.payload}")
