from flask import Flask, render_template, render_template_string, request, redirect, url_for, jsonify, g
from auth_middleware import token_required, admin_required
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from notification_template import NotificationTemplate
from notification_rule import NotificationRule
from compliance_deadline import ComplianceDeadline
from notification_audit_log import NotificationAuditLog
from datetime import datetime, timedelta
import requests
import json
import os
import threading
import time
from markupsafe import Markup

LOCAL_RULE_STORE = []
LOCAL_DEADLINE_STORE = []
LOCAL_AUDIT_LOG_STORE = []


def _clean_document_link(value):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _get_deadline_document_link(deadline):
    direct_link = _clean_document_link(getattr(deadline, 'document_link', None))
    if direct_link:
        return direct_link

    metadata_json = getattr(deadline, 'metadata_json', None)
    if isinstance(metadata_json, dict):
        return _clean_document_link(metadata_json.get('document_link'))

    return None


def _set_deadline_document_link(deadline, value):
    document_link = _clean_document_link(value)
    deadline.document_link = document_link

    metadata_json = getattr(deadline, 'metadata_json', None)
    if not isinstance(metadata_json, dict):
        metadata_json = {}

    if document_link:
        metadata_json['document_link'] = document_link
    else:
        metadata_json.pop('document_link', None)

    deadline.metadata_json = metadata_json


def _serialize_deadline(deadline):
    due_date = getattr(deadline, 'due_date', None)
    if isinstance(due_date, datetime):
        due_date = due_date.isoformat()

    return {
        'id': getattr(deadline, 'id', None),
        'entity_id': getattr(deadline, 'entity_id', None),
        'entity_type': getattr(deadline, 'entity_type', None),
        'deadline_type': getattr(deadline, 'deadline_type', None),
        'due_date': due_date,
        'status': getattr(deadline, 'status', None),
        'organization_id': getattr(deadline, 'organization_id', None),
        'document_link': _get_deadline_document_link(deadline)
    }


def _to_date(value):
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, 'date'):
        try:
            return value.date()
        except Exception:
            pass
    if isinstance(value, str):
        raw = value.strip()
        for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(raw).date()
        except ValueError:
            return None
    return None


def _extract_days_before_due(rule):
    direct_value = getattr(rule, 'days_before_due', None)
    if direct_value is not None:
        try:
            return int(direct_value)
        except (TypeError, ValueError):
            return None

    condition_json = getattr(rule, 'condition_json', None)
    if isinstance(condition_json, str):
        try:
            condition_json = json.loads(condition_json)
        except Exception:
            return None

    if isinstance(condition_json, dict):
        value = condition_json.get('days_before_due')
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    if isinstance(condition_json, list):
        for item in condition_json:
            if isinstance(item, dict) and 'days_before_due' in item:
                try:
                    return int(item.get('days_before_due'))
                except (TypeError, ValueError):
                    return None

    return None


def run_notification_engine(organization_id=None, include_results=False):
    organization_id = organization_id or get_organization_id()
    today = datetime.now().date()

    enabled_rules = [
        r for r in LOCAL_RULE_STORE
        if getattr(r, 'organization_id', None) == organization_id and getattr(r, 'enabled', False)
    ]
    deadlines = [
        d for d in LOCAL_DEADLINE_STORE
        if getattr(d, 'organization_id', None) == organization_id
    ]

    checked = 0
    triggered = 0
    skipped = 0
    results = []

    for deadline in deadlines:
        due_date = _to_date(getattr(deadline, 'due_date', None))
        for rule in enabled_rules:
            checked += 1
            days_before_due = _extract_days_before_due(rule)

            if due_date is None or days_before_due is None:
                skipped += 1
                if include_results:
                    results.append({
                        'status': 'skipped',
                        'deadline_id': getattr(deadline, 'id', None),
                        'rule_id': getattr(rule, 'id', None),
                        'message': 'skipped because not due today'
                    })
                continue

            trigger_date = due_date - timedelta(days=days_before_due)
            if today != trigger_date:
                skipped += 1
                if include_results:
                    results.append({
                        'status': 'skipped',
                        'deadline_id': getattr(deadline, 'id', None),
                        'rule_id': getattr(rule, 'id', None),
                        'message': 'skipped because not due today'
                    })
                continue

            duplicate_exists = any(
                log.get('organization_id') == organization_id
                and log.get('action') == 'NOTIFICATION_TRIGGERED'
                and str(log.get('deadline_id')) == str(getattr(deadline, 'id', None))
                and str(log.get('rule_id')) == str(getattr(rule, 'id', None))
                and str(log.get('trigger_date')) == today.isoformat()
                for log in LOCAL_AUDIT_LOG_STORE
            )
            if duplicate_exists:
                skipped += 1
                if include_results:
                    results.append({
                        'status': 'skipped',
                        'deadline_id': getattr(deadline, 'id', None),
                        'rule_id': getattr(rule, 'id', None),
                        'message': 'skipped because already logged today'
                    })
                continue

            template_name = None
            template_body_preview = None
            try:
                template = session.query(NotificationTemplate).filter_by(
                    id=getattr(rule, 'template_id', None),
                    organization_id=organization_id
                ).first()
                if template:
                    template_name = getattr(template, 'name', None)
                    template_body = getattr(template, 'body', None)
                    if template_body is not None:
                        template_body = str(template_body)
                        template_body_preview = (
                            template_body[:200] + '...'
                            if len(template_body) > 200
                            else template_body
                        )
            except Exception:
                template = None

            LOCAL_AUDIT_LOG_STORE.append({
                'timestamp': datetime.now(),
                'action': 'NOTIFICATION_TRIGGERED',
                'user': 'notification-engine',
                'details': (
                    f"Reminder triggered | vendor={getattr(deadline, 'entity_type', None)}"
                    f" | requirement={getattr(deadline, 'deadline_type', None)}"
                    f" | due_date={_to_date(getattr(deadline, 'due_date', None))}"
                    f" | document_link={_get_deadline_document_link(deadline) or 'N/A'}"
                    f" | deadline_id={getattr(deadline, 'id', None)}"
                    f" | rule_id={getattr(rule, 'id', None)}"
                    f" | template_name={template_name or 'N/A'}"
                    f" | template_content_preview={template_body_preview or 'N/A'}"
                ),
                'organization_id': organization_id,
                'deadline_id': getattr(deadline, 'id', None),
                'rule_id': getattr(rule, 'id', None),
                'trigger_date': today.isoformat()
            })
            triggered += 1
            if include_results:
                results.append({
                    'status': 'triggered',
                    'deadline_id': getattr(deadline, 'id', None),
                    'rule_id': getattr(rule, 'id', None),
                    'message': (
                        f"triggered | vendor={getattr(deadline, 'entity_type', None)}"
                        f" | requirement={getattr(deadline, 'deadline_type', None)}"
                        f" | due_date={_to_date(getattr(deadline, 'due_date', None))}"
                        f" | document_link={_get_deadline_document_link(deadline) or 'N/A'}"
                        f" | template_name={template_name or 'N/A'}"
                        f" | template_content_preview={template_body_preview or 'N/A'}"
                    )
                })

    summary = {
        'checked': checked,
        'triggered': triggered,
        'skipped': skipped
    }
    if include_results:
        summary['results'] = results
    return summary

def get_organization_id():
    return getattr(getattr(g, "current_user", None), "organization_id", 1)

# Example engine creation, replace with actual engine in use
engine = create_engine('sqlite:///notifications.db')
Session = sessionmaker(bind=engine)
session = Session()

@contextmanager
def get_session():
    session = Session()
    try:
        yield session
    finally:
        session.close()

app = Flask(__name__)

ADMIN_SHARED_CSS = """
<style>
    :root {
        --admin-bg: #f8fafc;
        --admin-surface: #ffffff;
        --admin-border: #e5e7eb;
        --admin-text: #0f172a;
        --admin-muted: #64748b;
        --admin-accent: #2563eb;
        --admin-accent-hover: #1d4ed8;
    }

    body {
        margin: 0;
        padding: 32px 24px;
        max-width: 1160px;
        margin-left: auto;
        margin-right: auto;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        background: var(--admin-bg);
        color: var(--admin-text);
        line-height: 1.45;
    }

    h1 {
        margin: 0 0 22px;
        font-size: 1.75rem;
        font-weight: 650;
        letter-spacing: -0.01em;
    }

    form {
        background: var(--admin-surface);
        border: 1px solid var(--admin-border);
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 18px;
    }

    label {
        display: block;
        margin: 10px 0 6px;
        font-size: 0.92rem;
        color: var(--admin-muted);
    }

    input,
    textarea,
    select,
    button {
        font: inherit;
    }

    input,
    textarea,
    select {
        box-sizing: border-box;
        width: 100%;
        padding: 9px 11px;
        margin: 0 0 10px;
        border: 1px solid var(--admin-border);
        border-radius: 8px;
        background: #fff;
    }

    textarea {
        min-height: 92px;
        resize: vertical;
    }

    button {
        border: 1px solid transparent;
        border-radius: 8px;
        padding: 9px 14px;
        background: var(--admin-accent);
        color: #fff;
        font-weight: 600;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.12);
        transition: background-color 0.15s ease, transform 0.08s ease, box-shadow 0.15s ease;
        cursor: pointer;
    }

    button:hover {
        background: var(--admin-accent-hover);
        box-shadow: 0 4px 10px rgba(37, 99, 235, 0.2);
    }

    button:active {
        transform: translateY(1px);
    }

    table {
        width: 100%;
        border-collapse: collapse;
        background: var(--admin-surface);
        border: 1px solid var(--admin-border);
        border-radius: 10px;
        overflow: hidden;
        font-size: 0.94rem;
    }

    th,
    td {
        border-bottom: 1px solid var(--admin-border);
        padding: 10px 12px;
        text-align: left;
        vertical-align: top;
    }

    th {
        background: #f1f5f9;
        font-weight: 600;
        color: #334155;
    }

    tbody tr:nth-child(even) {
        background: #f8fafc;
    }

    tbody tr:hover {
        background: #eef2ff;
    }

    tr:last-child td {
        border-bottom: none;
    }

    td form {
        background: transparent;
        border: 0;
        padding: 0;
        margin: 0 0 8px;
    }

    td form:last-child {
        margin-bottom: 0;
    }

    a {
        color: var(--admin-accent);
        text-decoration: none;
    }

    a:hover {
        text-decoration: underline;
    }

    .admin-nav {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        margin: 0 0 18px;
        padding: 10px 12px;
        border: 1px solid var(--admin-border);
        border-radius: 10px;
        background: var(--admin-surface);
    }

    .admin-nav-link {
        padding: 6px 10px;
        border-radius: 8px;
    }

    .admin-nav-link.active {
        background: #dbeafe;
        color: #1e3a8a;
        text-decoration: none;
        font-weight: 600;
    }

    .dashboard-summary {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 14px;
        margin: 0 0 20px;
    }

    .dashboard-summary-card {
        background: var(--admin-surface);
        border: 1px solid var(--admin-border);
        border-radius: 10px;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06);
        padding: 14px;
    }

    .dashboard-summary-label {
        color: var(--admin-muted);
        font-size: 0.9rem;
        margin-bottom: 4px;
    }

    .dashboard-summary-value {
        font-size: 1.4rem;
        font-weight: 650;
    }

    .dashboard-section {
        margin-bottom: 20px;
        background: var(--admin-surface);
        border: 1px solid var(--admin-border);
        border-radius: 12px;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06);
        padding: 16px;
    }

    .dashboard-section h2 {
        margin: 0 0 12px;
        font-size: 1.08rem;
        letter-spacing: -0.01em;
    }
</style>
"""


def _inject_admin_shared_css(rendered_html):
    if not rendered_html or ADMIN_SHARED_CSS.strip() in rendered_html:
        return rendered_html
    return rendered_html.replace('</head>', f'{ADMIN_SHARED_CSS}</head>')


def _inject_admin_navigation(rendered_html, current_path=''):
    if not rendered_html or 'data-admin-main-nav="true"' in rendered_html:
        return rendered_html

    nav_links = [
        ('/admin/dashboard', 'Dashboard'),
        ('/admin/deadlines', 'Deadlines'),
        ('/admin/audit-logs', 'Audit Logs'),
        ('/admin/rules', 'Rules'),
        ('/admin/templates', 'Templates')
    ]

    links_html = ''.join(
        (
            f'<a class="admin-nav-link{" active" if path == current_path else ""}" '
            f'href="{path}">{label}</a>'
        )
        for path, label in nav_links
    )
    nav_html = f'<nav class="admin-nav" data-admin-main-nav="true">{links_html}</nav>'
    return rendered_html.replace('<body>', f'<body>{nav_html}', 1)

_scheduler_lock = threading.Lock()
_scheduler_thread = None


def _notification_scheduler_loop():
    while True:
        try:
            with app.app_context():
                run_notification_engine()
        except Exception as exc:
            print(f"[notification-scheduler] Daily run failed: {exc}")

        time.sleep(24 * 60 * 60)


def start_notification_scheduler():
    global _scheduler_thread

    with _scheduler_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            return

        _scheduler_thread = threading.Thread(
            target=_notification_scheduler_loop,
            name='notification-engine-daily-scheduler',
            daemon=True
        )
        _scheduler_thread.start()

# Admin-only access control

@app.route('/api/templates', methods=['POST'])
def create_template():
    data = request.json
    if not data.get('name') or not data.get('subject') or not data.get('body'):
        return jsonify({'error': 'Name, subject, and body are required'}), 400

    organization_id = get_organization_id()
    template = NotificationTemplate(
        name=data['name'],
        subject=data['subject'],
        body=data['body'],
        required_variables=data.get('required_variables', ''),
        organization_id=organization_id
    )
    session.add(template)
    session.commit()
    return jsonify({'id': template.id}), 201

@app.route('/api/templates', methods=['GET'])
def list_templates():
    organization_id = get_organization_id()
    templates = session.query(NotificationTemplate).filter_by(organization_id=organization_id).all()
    return jsonify([{
        'id': template.id,
        'name': template.name,
        'subject': template.subject,
        'body': template.body,
        'required_variables': template.required_variables
    } for template in templates])

@app.route('/api/templates/<int:id>', methods=['PUT'])
def update_template(id):
    data = request.json
    organization_id = get_organization_id()
    template = session.query(NotificationTemplate).filter_by(id=id, organization_id=organization_id).first()
    if not template:
        return jsonify({'error': 'Template not found'}), 404

    if not data.get('name') or not data.get('subject') or not data.get('body'):
        return jsonify({'error': 'Name, subject, and body are required'}), 400

    template.name = data['name']
    template.subject = data['subject']
    template.body = data['body']
    template.required_variables = data.get('required_variables', '')
    session.commit()
    return jsonify({'id': template.id})

@admin_required
def templates_view():
    organization_id = get_organization_id()
    if request.method == 'POST':
        template_name = request.form.get('template_name')
        template_subject = request.form.get('template_subject') or request.form.get('template_content')
        template_body = request.form.get('template_body') or request.form.get('template_content')

        template = session.query(NotificationTemplate).filter_by(name=template_name, organization_id=organization_id).first()
        if template:
            template.subject = template_subject
            template.body = template_body
        else:
            template = NotificationTemplate(
                name=template_name,
                subject=template_subject,
                body=template_body,
                organization_id=organization_id
            )
            session.add(template)
        session.commit()
        return redirect(url_for('templates_view'))

    templates = session.query(NotificationTemplate).filter_by(organization_id=organization_id).all()
    rendered_html = render_template('templates.html', templates=templates)
    rendered_html = _inject_admin_shared_css(rendered_html)
    return _inject_admin_navigation(rendered_html, '/admin/templates')

@app.route('/api/demo/setup', methods=['POST'])
@admin_required
@token_required
def setup_demo():
    organization_id = get_organization_id()

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
        print("RULE CREATED", rule.__dict__)
        session.add(rule)
        session.commit()
        rules = NotificationRule.get_all()
        print("TOTAL RULES:", len(rules))
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

@app.route('/api/rules', methods=['GET'])
def list_rules():
    from notification_rule import NotificationRule

    organization_id = get_organization_id()

    rules = NotificationRule.get_all()

    return [r for r in rules if getattr(r, "organization_id", None) == organization_id]

@app.route('/api/rules/<int:id>', methods=['PUT'])
def update_rule(id):
    data = request.json
    organization_id = get_organization_id()
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
    organization_id = get_organization_id()
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
    organization_id = get_organization_id()
    rule = session.query(NotificationRule).filter_by(id=id, organization_id=organization_id).first()
    if not rule:
        return jsonify({'error': 'Rule not found'}), 404

    rule.enabled = False
    session.commit()
    return jsonify({'id': rule.id, 'enabled': rule.enabled})

@app.route('/api/notification-audit-logs', methods=['GET'])
def get_notification_audit_logs():
    organization_id = get_organization_id()
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

@app.route('/api/notification-health', methods=['GET'])
@admin_required
@token_required
def notification_health():
    organization_id = get_organization_id()
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

    organization_id = get_organization_id()
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

@app.route('/api/deadlines', methods=['GET'])
def get_deadlines():
    deadlines = session.query(ComplianceDeadline).all()
    return jsonify([_serialize_deadline(deadline) for deadline in deadlines])

@app.route('/api/deadlines', methods=['POST'])
def create_deadline():
    data = request.json
    new_deadline = ComplianceDeadline(
        entity_id=data['entity_id'],
        entity_type=data['entity_type'],
        deadline_type=data['deadline_type'],
        due_date=data['due_date'],
        status=data.get('status', 'PENDING'),
        organization_id=data['organization_id']
    )
    _set_deadline_document_link(new_deadline, data.get('document_link'))
    session.add(new_deadline)
    session.commit()
    return jsonify(_serialize_deadline(new_deadline)), 201

@app.route('/api/deadlines/<int:id>', methods=['PUT'])
def update_deadline(id):
    data = request.json
    deadline = session.query(ComplianceDeadline).filter_by(id=id).first()
    if not deadline:
        return jsonify({'error': 'Deadline not found'}), 404

    deadline.entity_id = data.get('entity_id', deadline.entity_id)
    deadline.entity_type = data.get('entity_type', deadline.entity_type)
    deadline.deadline_type = data.get('deadline_type', deadline.deadline_type)
    deadline.due_date = data.get('due_date', deadline.due_date)
    deadline.status = data.get('status', deadline.status)
    deadline.organization_id = data.get('organization_id', deadline.organization_id)
    if 'document_link' in data:
        _set_deadline_document_link(deadline, data.get('document_link'))

    session.commit()
    return jsonify(_serialize_deadline(deadline))
@admin_required
def rules_view():
    if request.method == 'POST':
        # Handle create/edit/enable/disable rule logic
        rule_name = request.form.get('rule_name')
        rule_description = request.form.get('rule_description')
        # Call API to create or update rule
        response = requests.post(f"{BASE_URL}/api/rules", json={
            'name': rule_name,
            'description': rule_description
        })
        if response.ok:
            return redirect(url_for('rules_view'))
        else:
            # Handle error
            pass
    # Fetch rules from API
    response = requests.get(f"{BASE_URL}/api/rules")
    rules = response.json() if response.ok else []
    rendered_html = render_template('rules.html', rules=rules)
    rendered_html = _inject_admin_shared_css(rendered_html)
    return _inject_admin_navigation(rendered_html, '/admin/rules')

@app.route('/admin/templates', methods=['GET', 'POST'], endpoint='templates_view')
# @admin_required
def templates_view():
    organization_id = get_organization_id()
    if request.method == 'POST':
        template_name = request.form.get('template_name')
        template_subject = request.form.get('template_subject') or request.form.get('template_content')
        template_body = request.form.get('template_body') or request.form.get('template_content')

        template = session.query(NotificationTemplate).filter_by(name=template_name, organization_id=organization_id).first()
        if template:
            template.subject = template_subject
            template.body = template_body
        else:
            template = NotificationTemplate(
                name=template_name,
                subject=template_subject,
                body=template_body,
                organization_id=organization_id
            )
            session.add(template)
        session.commit()
        return redirect(url_for('templates_view'))

    # Fetch templates directly from the database
    templates = session.query(NotificationTemplate).filter_by(organization_id=organization_id).all()
    rendered_html = render_template('templates.html', templates=templates)
    rendered_html = _inject_admin_shared_css(rendered_html)
    return _inject_admin_navigation(rendered_html, '/admin/templates')

@app.route('/admin/rules', methods=['GET', 'POST'], endpoint='rules_view')
# @admin_required
def rules_view():
    organization_id = get_organization_id()

    if request.method == 'POST':
        rule_name = request.form.get('rule_name')
        rule_description = request.form.get('rule_description')
        rule_event_type = request.form.get('event_type', 'DEADLINE_UPCOMING')
        rule_template_id = request.form.get('template_id', 1)

        rule = NotificationRule(
            id=request.form.get('id', len(NotificationRule.get_all()) + 1),
            name=rule_name,
            event_type=rule_event_type,
            condition_json=rule_description,
            template_id=rule_template_id,
            enabled=True,
            organization_id=organization_id
        )
        LOCAL_RULE_STORE.append(rule)
        print("ALL RULES AFTER CREATE:", len(NotificationRule.get_all()))
        print("RULE CREATED")
        return redirect(url_for("rules_view"))

    rules = [r for r in LOCAL_RULE_STORE if getattr(r, "organization_id", None) == organization_id]
    print("RULES BEING SENT TO TEMPLATE:", rules)

    rendered_html = render_template('rules.html', rules=rules)
    rendered_html = _inject_admin_shared_css(rendered_html)
    return _inject_admin_navigation(rendered_html, '/admin/rules')

@app.route('/admin/audit-logs', methods=['GET'], endpoint='audit_logs_view')
# @admin_required
def audit_logs_view():
    organization_id = get_organization_id()
    filter_param = (request.args.get('filter', '') or '').strip().lower()

    def format_timestamp(value):
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return str(value or '')

    try:
        logs = session.query(NotificationAuditLog).filter_by(organization_id=organization_id).all()
    except Exception:
        # Safe local-dev fallback if audit table/schema is not ready
        logs = []

    if filter_param:
        logs = [
            log for log in logs
            if (
                filter_param in (str(getattr(log, 'event_type', ''))).lower()
                or filter_param in (str(getattr(log, 'recipient', ''))).lower()
                or filter_param in (str(getattr(log, 'status', ''))).lower()
                or filter_param in (str(getattr(log, 'error_message', ''))).lower()
            )
        ]

    audit_logs = [
        {
            'timestamp': format_timestamp(log.sent_at),
            'action': log.event_type or 'NOTIFICATION_EVENT',
            'user': log.recipient,
            'details': log.error_message or log.status
        }
        for log in logs
    ]

    audit_logs.insert(0, {
        'timestamp': '',
        'action': 'NOTIFICATION_ENGINE',
        'user': 'admin',
        'details': Markup('<a href="/admin/run-test-notification">Run notification engine now</a>')
    })

    local_audit_logs = [
        {
            **log,
            'timestamp': format_timestamp(log.get('timestamp')),
            'action': log.get('action') or 'TEST_NOTIFICATION',
            'details': log.get('details') or ''
        }
        for log in LOCAL_AUDIT_LOG_STORE
        if log.get('organization_id') == organization_id
    ]

    if filter_param:
        local_audit_logs = [
            log for log in local_audit_logs
            if (
                filter_param in (str(log.get('action', ''))).lower()
                or filter_param in (str(log.get('user', ''))).lower()
                or filter_param in (str(log.get('details', ''))).lower()
            )
        ]

    audit_logs.extend(local_audit_logs)

    rendered_html = render_template('audit_logs.html', audit_logs=audit_logs)
    rendered_html = _inject_admin_shared_css(rendered_html)
    return _inject_admin_navigation(rendered_html, '/admin/audit-logs')

@app.route('/admin/run-test-notification', methods=['GET'])
def run_test_notification():
    organization_id = get_organization_id()

    result = run_notification_engine(organization_id, include_results=True)
    LOCAL_AUDIT_LOG_STORE.append({
        'timestamp': datetime.now(),
        'action': 'RUN_TEST_NOTIFICATION',
        'user': 'local-test',
        'details': (
            f"checked={result['checked']} | triggered={result['triggered']} | skipped={result['skipped']}"
        ),
        'organization_id': organization_id
    })

    for item in result.get('results', []):
        LOCAL_AUDIT_LOG_STORE.append({
            'timestamp': datetime.now(),
            'action': f"RUN_TEST_{str(item.get('status', '')).upper()}",
            'user': 'notification-engine',
            'details': (
                f"deadline_id={item.get('deadline_id')} | rule_id={item.get('rule_id')} | {item.get('message')}"
            ),
            'organization_id': organization_id
        })

    return redirect(url_for('audit_logs_view'))

@app.route('/admin/deadlines', methods=['GET', 'POST'], endpoint='deadlines_view')
# @admin_required
def deadlines_view():
    organization_id = get_organization_id()

    if request.method == 'POST':
        # Read form data
        entity_id = request.form.get('entity_id')
        entity_type = request.form.get('entity_type')
        deadline_type = request.form.get('deadline_type')
        due_date = request.form.get('due_date')
        status = request.form.get('status', 'PENDING')
        document_link = request.form.get('document_link')

        # Create and save new deadline (local fallback for dev)
        new_deadline = ComplianceDeadline(
            id=request.form.get('id', len(LOCAL_DEADLINE_STORE) + 1),
            entity_id=entity_id,
            entity_type=entity_type,
            deadline_type=deadline_type,
            due_date=due_date,
            status=status,
            organization_id=organization_id
        )
        _set_deadline_document_link(new_deadline, document_link)
        LOCAL_DEADLINE_STORE.append(new_deadline)

        # Redirect back to /admin/deadlines
        return redirect(url_for('deadlines_view'))

    # Fetch deadlines to display in the table (local fallback for dev)
    deadlines = [
        d for d in LOCAL_DEADLINE_STORE
        if getattr(d, "organization_id", None) == organization_id
    ]

    rendered_html = render_template('admin_deadlines.html', deadlines=deadlines)
    rendered_html = _inject_admin_shared_css(rendered_html)
    rendered_html = _inject_admin_navigation(rendered_html, '/admin/deadlines')
    document_link_ui_patch = """
    <script>
        (function () {
            function addDocumentLinkFormField() {
                const form = document.getElementById('deadline-form');
                if (!form || document.getElementById('document-link')) return;

                const label = document.createElement('label');
                label.setAttribute('for', 'document-link');
                label.textContent = 'Document Link:';

                const input = document.createElement('input');
                input.type = 'url';
                input.name = 'document_link';
                input.id = 'document-link';
                input.placeholder = 'https://...';

                const saveButton = form.querySelector('button[type="submit"]');
                if (saveButton) {
                    form.insertBefore(label, saveButton);
                    form.insertBefore(input, saveButton);
                } else {
                    form.appendChild(label);
                    form.appendChild(input);
                }
            }

            function addDocumentLinkHeader() {
                const headerRow = document.querySelector('table thead tr');
                if (!headerRow || document.getElementById('document-link-header')) return;

                const th = document.createElement('th');
                th.id = 'document-link-header';
                th.textContent = 'Document Link';

                const actionsHeader = Array.from(headerRow.children).find(function (cell) {
                    return (cell.textContent || '').trim() === 'Actions';
                });

                if (actionsHeader) {
                    headerRow.insertBefore(th, actionsHeader);
                } else {
                    headerRow.appendChild(th);
                }
            }

            window.fetchDeadlines = function () {
                fetch('/api/deadlines', {
                    headers: {
                        'Authorization': 'Bearer valid-token'
                    }
                })
                .then(response => response.json())
                .then(data => {
                    const tableBody = document.getElementById('deadlines-table-body');
                    tableBody.innerHTML = '';
                    data.forEach(deadline => {
                        const row = document.createElement('tr');
                        const hasDocumentLink = !!deadline.document_link;
                        const linkCell = hasDocumentLink
                            ? `<a href="${deadline.document_link}" target="_blank" rel="noopener noreferrer">${deadline.document_link}</a>`
                            : '';

                        row.innerHTML = `
                            <td>${deadline.id}</td>
                            <td>${deadline.entity_id}</td>
                            <td>${deadline.entity_type}</td>
                            <td>${deadline.deadline_type}</td>
                            <td>${new Date(deadline.due_date).toLocaleString()}</td>
                            <td>${deadline.status}</td>
                            <td>${linkCell}</td>
                            <td>
                                <button onclick="editDeadline(${deadline.id})">Edit</button>
                                <button onclick="markAsCompleted(${deadline.id})">Mark as Completed</button>
                            </td>
                        `;
                        tableBody.appendChild(row);
                    });
                })
                .catch(error => console.error('Error:', error));
            };

            addDocumentLinkFormField();
            addDocumentLinkHeader();
            window.fetchDeadlines();
        })();
    </script>
    """

    return rendered_html.replace('</body>', f'{document_link_ui_patch}</body>')


@app.route('/admin/dashboard', methods=['GET'], endpoint='dashboard_view')
def dashboard_view():
    organization_id = get_organization_id()
    today = datetime.now().date()
    recent_cutoff = datetime.now() - timedelta(days=7)

    deadlines = [
        d for d in LOCAL_DEADLINE_STORE
        if getattr(d, 'organization_id', None) == organization_id
    ]

    due_today_count = 0
    overdue_count = 0
    needs_attention_rows = []
    deadline_rows = []

    for deadline in deadlines:
        due_date = _to_date(getattr(deadline, 'due_date', None))
        status = str(getattr(deadline, 'status', '') or '').upper()

        if due_date == today:
            due_today_count += 1

        if status == 'OVERDUE' or (due_date and due_date < today and status != 'COMPLETED'):
            overdue_count += 1

        is_overdue = due_date and due_date < today and status != 'COMPLETED'
        is_due_today = due_date == today and status != 'COMPLETED'
        is_due_soon = due_date and today < due_date <= (today + timedelta(days=7)) and status != 'COMPLETED'

        if is_overdue or is_due_today or is_due_soon:
            attention_label = 'Overdue' if is_overdue else ('Due Today' if is_due_today else 'Due Soon')
            needs_attention_rows.append({
                'id': getattr(deadline, 'id', ''),
                'entity_type': getattr(deadline, 'entity_type', ''),
                'deadline_type': getattr(deadline, 'deadline_type', ''),
                'due_date': due_date.isoformat() if due_date else str(getattr(deadline, 'due_date', '') or ''),
                'status': getattr(deadline, 'status', ''),
                'attention': attention_label
            })

        deadline_rows.append({
            'id': getattr(deadline, 'id', ''),
            'entity_id': getattr(deadline, 'entity_id', ''),
            'entity_type': getattr(deadline, 'entity_type', ''),
            'deadline_type': getattr(deadline, 'deadline_type', ''),
            'due_date': due_date.isoformat() if due_date else str(getattr(deadline, 'due_date', '') or ''),
            'status': getattr(deadline, 'status', ''),
            'document_link': _get_deadline_document_link(deadline)
        })

    def _safe_timestamp(value):
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return datetime.min
            try:
                return datetime.fromisoformat(raw)
            except ValueError:
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                    try:
                        return datetime.strptime(raw, fmt)
                    except ValueError:
                        continue
        return datetime.min

    audit_rows = []
    try:
        logs = session.query(NotificationAuditLog).filter_by(organization_id=organization_id).all()
    except Exception:
        logs = []

    for log in logs:
        sent_at = getattr(log, 'sent_at', None)
        audit_rows.append({
            'timestamp': sent_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(sent_at, datetime) else str(sent_at or ''),
            'action': getattr(log, 'event_type', None) or 'NOTIFICATION_EVENT',
            'user': getattr(log, 'recipient', None),
            'details': getattr(log, 'error_message', None) or getattr(log, 'status', None),
            '_sort_ts': _safe_timestamp(sent_at)
        })

    for log in LOCAL_AUDIT_LOG_STORE:
        if log.get('organization_id') != organization_id:
            continue
        ts = log.get('timestamp')
        audit_rows.append({
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S') if isinstance(ts, datetime) else str(ts or ''),
            'action': log.get('action') or 'TEST_NOTIFICATION',
            'user': log.get('user'),
            'details': log.get('details') or '',
            '_sort_ts': _safe_timestamp(ts)
        })

    recent_reminders_sent_count = 0
    for row in audit_rows:
        action_value = str(row.get('action') or '').upper()
        details_value = str(row.get('details') or '').upper()
        row_ts = row.get('_sort_ts', datetime.min)
        if row_ts >= recent_cutoff and (
            'NOTIFICATION' in action_value
            or 'REMINDER' in action_value
            or 'NOTIFICATION' in details_value
            or 'REMINDER' in details_value
            or 'SENT' in details_value
        ):
            recent_reminders_sent_count += 1

    needs_attention_rows = sorted(
        needs_attention_rows,
        key=lambda row: _safe_timestamp(row.get('due_date'))
    )[:10]

    recent_audit_rows = sorted(audit_rows, key=lambda row: row.get('_sort_ts', datetime.min), reverse=True)[:10]

    dashboard_html = render_template_string(
        """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Admin Dashboard</title>
        </head>
        <body>
            <h1>Admin Dashboard</h1>

            <section class="dashboard-summary">
                <div class="dashboard-summary-card">
                    <div class="dashboard-summary-label">Total Compliance Items</div>
                    <div class="dashboard-summary-value">{{ total_deadlines }}</div>
                </div>
                <div class="dashboard-summary-card">
                    <div class="dashboard-summary-label">Due Today</div>
                    <div class="dashboard-summary-value">{{ due_today_count }}</div>
                </div>
                <div class="dashboard-summary-card">
                    <div class="dashboard-summary-label">Overdue</div>
                    <div class="dashboard-summary-value">{{ overdue_count }}</div>
                </div>
                <div class="dashboard-summary-card">
                    <div class="dashboard-summary-label">Recent Reminders Sent (7d)</div>
                    <div class="dashboard-summary-value">{{ recent_reminders_sent_count }}</div>
                </div>
            </section>

            <section class="dashboard-section">
                <h2>Needs Attention</h2>
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Entity Type</th>
                            <th>Requirement</th>
                            <th>Due Date</th>
                            <th>Status</th>
                            <th>Attention</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% if needs_attention_rows %}
                            {% for row in needs_attention_rows %}
                            <tr>
                                <td>{{ row.id }}</td>
                                <td>{{ row.entity_type }}</td>
                                <td>{{ row.deadline_type }}</td>
                                <td>{{ row.due_date }}</td>
                                <td>{{ row.status }}</td>
                                <td>{{ row.attention }}</td>
                            </tr>
                            {% endfor %}
                        {% else %}
                            <tr>
                                <td colspan="6">No items need attention right now.</td>
                            </tr>
                        {% endif %}
                    </tbody>
                </table>
            </section>

            <section class="dashboard-section">
                <h2>All Compliance Items</h2>
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Entity ID</th>
                            <th>Entity Type</th>
                            <th>Deadline Type</th>
                            <th>Due Date</th>
                            <th>Status</th>
                            <th>Document Link</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% if deadline_rows %}
                            {% for deadline in deadline_rows %}
                            <tr>
                                <td>{{ deadline.id }}</td>
                                <td>{{ deadline.entity_id }}</td>
                                <td>{{ deadline.entity_type }}</td>
                                <td>{{ deadline.deadline_type }}</td>
                                <td>{{ deadline.due_date }}</td>
                                <td>{{ deadline.status }}</td>
                                <td>
                                    {% if deadline.document_link %}
                                        <a href="{{ deadline.document_link }}" target="_blank" rel="noopener noreferrer">{{ deadline.document_link }}</a>
                                    {% endif %}
                                </td>
                            </tr>
                            {% endfor %}
                        {% else %}
                            <tr>
                                <td colspan="7">No deadlines available.</td>
                            </tr>
                        {% endif %}
                    </tbody>
                </table>
            </section>

            <section class="dashboard-section">
                <h2>Recent Activity</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Timestamp</th>
                            <th>Action</th>
                            <th>User</th>
                            <th>Details</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% if recent_audit_rows %}
                            {% for log in recent_audit_rows %}
                            <tr>
                                <td>{{ log.timestamp }}</td>
                                <td>{{ log.action }}</td>
                                <td>{{ log.user }}</td>
                                <td>{{ log.details }}</td>
                            </tr>
                            {% endfor %}
                        {% else %}
                            <tr>
                                <td colspan="4">No audit logs available.</td>
                            </tr>
                        {% endif %}
                    </tbody>
                </table>
            </section>
        </body>
        </html>
        """,
        total_deadlines=len(deadline_rows),
        due_today_count=due_today_count,
        overdue_count=overdue_count,
        recent_reminders_sent_count=recent_reminders_sent_count,
        needs_attention_rows=needs_attention_rows,
        deadline_rows=deadline_rows,
        recent_audit_rows=recent_audit_rows
    )

    dashboard_html = _inject_admin_shared_css(dashboard_html)
    return _inject_admin_navigation(dashboard_html, '/admin/dashboard')

if __name__ == '__main__':
    # Prevent duplicate start when Flask debug reloader spawns a parent process.
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
        start_notification_scheduler()

    app.run(debug=True)
