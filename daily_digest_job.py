from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from notification_digest_item import NotificationDigestItem
from email_notification_channel import EmailNotificationChannel
from notification_audit_log import NotificationAuditLog

def run_daily_digest(sendgrid_service):
    # Set up database session
    engine = create_engine('sqlite:///notifications.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query pending digest items
    pending_items = session.query(NotificationDigestItem).filter(
        (NotificationDigestItem.status == 'QUEUED_DIGEST') |
        ((NotificationDigestItem.status == 'DEFERRED_QUIET_HOURS') & (NotificationDigestItem.pending_send_at <= datetime.utcnow()))
    ).all()

    # Group items by recipient
    items_by_recipient = {}
    for item in pending_items:
        if item.recipient not in items_by_recipient:
            items_by_recipient[item.recipient] = []
        items_by_recipient[item.recipient].append(item)

    # Send combined email for each recipient
    channel = EmailNotificationChannel(sendgrid_service)
    for recipient, items in items_by_recipient.items():
        try:
            # Combine payloads
            combined_payload = {"items": [item.payload_json for item in items]}
            # Send email
            channel.send(recipient, "digest_template_id", combined_payload)
            # Mark items as SENT
            for item in items:
                item.status = 'SENT'
                item.pending_send_at = None  # Clear pending_send_at after sending
                # Log SUCCESS
                audit_log = NotificationAuditLog(
                    rule_id=None,
                    event_type=item.event_type,
                    recipient=item.recipient,
                    template_id=item.template_id,
                    status='SUCCESS'
                )
                session.add(audit_log)
        except Exception as e:
            # Log FAILED
            for item in items:
                audit_log = NotificationAuditLog(
                    rule_id=None,
                    event_type=item.event_type,
                    recipient=item.recipient,
                    template_id=item.template_id,
                    status='FAILED',
                    error_message=str(e)
                )
                session.add(audit_log)

    # Commit changes
    session.commit()