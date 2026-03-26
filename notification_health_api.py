from flask import Flask, jsonify, request
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, and_
from datetime import datetime
from notification_audit_log import NotificationAuditLog, Base


# Database setup
engine = create_engine('sqlite:///notifications.db')  # Update with actual database URI
Session = sessionmaker(bind=engine)
session = Session()

@app.route('/api/notification-health', methods=['GET'])
from auth_middleware import token_required, admin_required

@admin_required
@token_required
def notification_health():
    organization_id = g.current_user.organization_id
    summary = get_notification_summary(organization_id)
    return jsonify(summary)

@app.route('/api/notification-recovery/requeue-failed', methods=['POST'])
@admin_required
@token_required
def requeue_failed_notifications():
    filters = []
    event_type = request.json.get('event_type')
    recipient = request.json.get('recipient')
    limit = request.json.get('limit')

    if event_type:
        filters.append(NotificationAuditLog.event_type == event_type)
    if recipient:
        filters.append(NotificationAuditLog.recipient == recipient)

    organization_id = g.current_user.organization_id
    query = session.query(NotificationAuditLog).filter(
        NotificationAuditLog.organization_id == organization_id,
        NotificationAuditLog.status.in_(["FAILED", "FAILED_PERMANENT"]),
        *filters
    )

    if limit:
        query = query.limit(limit)

    if query.count() == 0:
        return jsonify({'error': 'No records found for the current organization'}), 404

    matched_count = query.count()
    requeued_count = 0

    for log in query.all():
        log.status = "FAILED"
        log.next_retry_at = datetime.now()
        requeued_count += 1

    session.commit()

    return jsonify({
        "matched_count": matched_count,
        "requeued_count": requeued_count
    })

