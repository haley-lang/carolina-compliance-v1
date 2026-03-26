from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from notification_rule import NotificationRule
from compliance_deadline import ComplianceDeadline
from notification_audit_log import NotificationAuditLog
from notification_digest_item import NotificationDigestItem

def get_notification_summary():
    # Set up database session
    engine = create_engine('sqlite:///notifications.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    # Count rules by enabled/disabled
    enabled_rules_count = session.query(NotificationRule).filter_by(enabled=True).count()
    disabled_rules_count = session.query(NotificationRule).filter_by(enabled=False).count()

    # Count pending deadlines
    pending_deadlines_count = session.query(ComplianceDeadline).filter_by(status='PENDING').count()

    # Count overdue deadlines
    overdue_deadlines_count = session.query(ComplianceDeadline).filter_by(status='OVERDUE').count()

    # Count failed notifications
    failed_notifications_count = session.query(NotificationAuditLog).filter_by(status='FAILED').count()

    # Count failed_permanent notifications
    failed_permanent_count = session.query(NotificationAuditLog).filter_by(status='FAILED_PERMANENT').count()

    # Count queued_digest notifications
    queued_digest_count = session.query(NotificationDigestItem).filter_by(status='QUEUED_DIGEST').count()

    # Return summary as JSON
    return {
        "enabled_rules_count": enabled_rules_count,
        "disabled_rules_count": disabled_rules_count,
        "pending_deadlines_count": pending_deadlines_count,
        "overdue_deadlines_count": overdue_deadlines_count,
        "failed_notifications_count": failed_notifications_count,
        "failed_permanent_count": failed_permanent_count,
        "queued_digest_count": queued_digest_count
    }