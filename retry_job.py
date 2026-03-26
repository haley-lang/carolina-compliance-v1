from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from notification_audit_log import NotificationAuditLog
from notification_engine import NotificationEngine

# Configuration
MAX_ATTEMPTS = 3
BACKOFF_MINUTES = 5

def retry_failed_notifications(engine, sendgrid_service):
    # Set up database session
    engine = create_engine('sqlite:///notifications.db')  # Example connection string
    Session = sessionmaker(bind=engine)
    session = Session()

    # Load FAILED logs where next_retry_at <= now
    failed_logs = session.query(NotificationAuditLog).filter(
        NotificationAuditLog.status == 'FAILED',
        NotificationAuditLog.next_retry_at <= datetime.utcnow()
    ).all()

    for log in failed_logs:
        # Retry through existing NotificationChannel flow
        notification_engine = NotificationEngine([], sendgrid_service)
        event = {
            'type': log.event_type,
            'payload': {
                'recipient': log.recipient,
                'template_id': log.template_id
            }
        }

        try:
            notification_engine.processEvent(event)
            log.status = 'SUCCESS'
            log.error_message = None
        except Exception as e:
            log.retry_count += 1
            if log.retry_count >= MAX_ATTEMPTS:
                log.status = 'FAILED_PERMANENT'
            else:
                log.next_retry_at = datetime.utcnow() + timedelta(minutes=BACKOFF_MINUTES * log.retry_count)
            log.error_message = str(e)

        session.commit()

# Example usage
# retry_failed_notifications(engine, sendgrid_service)