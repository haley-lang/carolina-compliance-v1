from flask import Flask, request, jsonify
from auth_middleware import token_required, admin_required
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from notification_template import NotificationTemplate, Base


# Example engine creation, replace with actual engine in use
engine = create_engine('sqlite:///notifications.db')
Session = sessionmaker(bind=engine)
session = Session()

@app.route('/api/templates', methods=['POST'])
@admin_required
@token_required
def create_template():
    data = request.json
    if not data.get('name') or not data.get('subject') or not data.get('body'):
        return jsonify({'error': 'Name, subject, and body are required'}), 400

    organization_id = g.current_user.organization_id
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
@admin_required
@token_required
def list_templates():
    organization_id = g.current_user.organization_id
    templates = session.query(NotificationTemplate).filter_by(organization_id=organization_id).all()
    return jsonify([{
        'id': template.id,
        'name': template.name,
        'subject': template.subject,
        'body': template.body,
        'required_variables': template.required_variables
    } for template in templates])

@app.route('/api/templates/<int:id>', methods=['PUT'])
@admin_required
@token_required
def update_template(id):
    data = request.json
    organization_id = g.current_user.organization_id
    template = session.query(NotificationTemplate).filter_by(id=id, organization_id=organization_id).first()
    if not template:
        return jsonify({'error': 'Template not found'}), 404
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

