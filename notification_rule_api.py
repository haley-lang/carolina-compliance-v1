from flask import Flask, request, jsonify, g
from datetime import datetime, timedelta
from notification_template import NotificationTemplate
from compliance_deadline import ComplianceDeadline
from auth_middleware import token_required, admin_required
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from notification_rule import NotificationRule, Base
from notification_audit_log import NotificationAuditLog


# Example engine creation, replace with actual engine in use
engine = create_engine('sqlite:///notifications.db')
Session = sessionmaker(bind=engine)
session = Session()

@app.route('/api/demo/setup', methods=['POST'])
@admin_required
@token_required
def setup_demo():
    organization_id = g.current_user.organization_id

    # Check for existing NotificationTemplate
    existing_template = session.query(NotificationTemplate).filter_by(name='Demo Template', organization_id=organization_id).first()
    if existing_template:
        template_id = existing_template.id
    else:
        # Create NotificationTemplate
        template = NotificationTemplate(
            name='Demo Template',
            subject='Demo Subject',
            body='Demo Body',
            organization_id=organization_id
        )
        session.add(template)
        session.commit()
        template_id = template.id

    # Check for existing NotificationRule
    existing_rule = session.query(NotificationRule).filter_by(event_type='DEADLINE_UPCOMING', template_id=template_id, organization_id=organization_id).first()
    if existing_rule:
        rule_id = existing_rule.id
    else:
        # Create NotificationRule
        rule = NotificationRule(
            name='Demo Rule',
            event_type='DEADLINE_UPCOMING',
            template_id=template_id,
            organization_id=organization_id
        )
        session.add(rule)
        session.commit()
        rule_id = rule.id

    # Check for existing ComplianceDeadline
    existing_deadline = session.query(ComplianceDeadline).filter_by(deadline_type='DEMO_DEADLINE', organization_id=organization_id).first()
    if existing_deadline:
        deadline_id = existing_deadline.id
    else:
        # Create ComplianceDeadline
        deadline = ComplianceDeadline(
            entity_id=1,  # Example entity_id
            entity_type='Demo Entity',
            deadline_type='DEMO_DEADLINE',
            due_date=datetime.now() + timedelta(days=2),
            organization_id=organization_id
        )
        session.add(deadline)
        session.commit()
        deadline_id = deadline.id

    return jsonify({
        'template_id': template_id,
        'rule_id': rule_id,
        'deadline_id': deadline_id
    }), 201

@admin_required
@token_required
def create_rule():
    data = request.json
    if not data.get('name') or not data.get('event_type') or not data.get('template_id'):
        return jsonify({'error': 'Name, event_type, and template_id are required'}), 400

    organization_id = g.current_user.organization_id
    rule = NotificationRule(
        name=data['name'],
        event_type=data['event_type'],
        template_id=data['template_id'],
        condition_json=data.get('condition_json', ''),
        required_variables=data.get('required_variables', ''),
        escalation_delay_days=data.get('escalation_delay_days'),
        escalation_recipient=data.get('escalation_recipient'),
        enabled=data.get('enabled', True),
        organization_id=organization_id
    )
    session.add(rule)
    session.commit()
    return jsonify({'id': rule.id}), 201

@app.route('/api/rules', methods=['GET'])
@admin_required
@token_required
def list_rules():
    organization_id = g.current_user.organization_id
    rules = session.query(NotificationRule).filter_by(organization_id=organization_id).all()
    return jsonify([{
        'id': rule.id,
        'name': rule.name,
        'event_type': rule.event_type,
        'template_id': rule.template_id,
        'condition_json': rule.condition_json,
        'required_variables': rule.required_variables,
        'escalation_delay_days': rule.escalation_delay_days,
        'escalation_recipient': rule.escalation_recipient,
        'enabled': rule.enabled
    } for rule in rules])

@app.route('/api/rules/<int:id>', methods=['PUT'])
@admin_required
@token_required
def update_rule(id):
    data = request.json
    organization_id = g.current_user.organization_id
    rule = session.query(NotificationRule).filter_by(id=id, organization_id=organization_id).first()
    if not rule:
        return jsonify({'error': 'Rule not found'}), 404

    if not data.get('name') or not data.get('event_type') or not data.get('template_id'):
        return jsonify({'error': 'Name, event_type, and template_id are required'}), 400

    rule.name = data['name']
    rule.event_type = data['event_type']
    rule.template_id = data['template_id']
    rule.condition_json = data.get('condition_json', rule.condition_json)
    rule.required_variables = data.get('required_variables', rule.required_variables)
    rule.escalation_delay_days = data.get('escalation_delay_days', rule.escalation_delay_days)
    rule.escalation_recipient = data.get('escalation_recipient', rule.escalation_recipient)
    rule.enabled = data.get('enabled', rule.enabled)
    session.commit()
    return jsonify({'id': rule.id})

@app.route('/api/rules/<int:id>/enable', methods=['POST'])
@admin_required
@token_required
def enable_rule(id):
    organization_id = g.current_user.organization_id
    rule = session.query(NotificationRule).filter_by(id=id, organization_id=organization_id).first()
    if not rule:
        return jsonify({'error': 'Rule not found'}), 404

    rule.enabled = True
    session.commit()
    return jsonify({'id': rule.id, 'enabled': rule.enabled})

@app.route('/api/rules/<int:id>/disable', methods=['POST'])
@admin_required
@token_required
def disable_rule(id):
    organization_id = g.current_user.organization_id
    rule = session.query(NotificationRule).filter_by(id=id, organization_id=organization_id).first()
    if not rule:
        return jsonify({'error': 'Rule not found'}), 404

    rule.enabled = False
    session.commit()
    return jsonify({'id': rule.id, 'enabled': rule.enabled})

@app.route('/api/notification-audit-logs', methods=['GET'])
@admin_required
@token_required
def get_notification_audit_logs():
    organization_id = g.current_user.organization_id
    query = session.query(NotificationAuditLog).filter_by(organization_id=organization_id)

    # Apply filters
    status = request.args.get('status')
    if status:
        query = query.filter(NotificationAuditLog.status == status)

    event_type = request.args.get('event_type')
    if event_type:
        query = query.filter(NotificationAuditLog.event_type == event_type)

    recipient = request.args.get('recipient')
    if recipient:
        query = query.filter(NotificationAuditLog.recipient == recipient)

    date_from = request.args.get('date_from')
    if date_from:
        query = query.filter(NotificationAuditLog.sent_at >= datetime.fromisoformat(date_from))

    date_to = request.args.get('date_to')
    if date_to:
        query = query.filter(NotificationAuditLog.sent_at <= datetime.fromisoformat(date_to))

    limit = request.args.get('limit', type=int)
    offset = request.args.get('offset', type=int)

    logs = query.limit(limit).offset(offset).all()

    return jsonify([{
        'id': log.id,
        'rule_id': log.rule_id,
        'event_type': log.event_type,
        'recipient': log.recipient,
        'template_id': log.template_id,
        'status': log.status,
        'sent_at': log.sent_at,
        'error_message': log.error_message,
        'retry_count': log.retry_count,
        'next_retry_at': log.next_retry_at
    } for log in logs])

