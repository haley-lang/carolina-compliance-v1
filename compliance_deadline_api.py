from flask import Blueprint, request, jsonify
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from compliance_deadline import ComplianceDeadline, Base

# Assuming an engine is already created
engine = create_engine('sqlite:///compliance.db')
Session = sessionmaker(bind=engine)
session = Session()

compliance_deadline_api = Blueprint('compliance_deadline_api', __name__)

@compliance_deadline_api.route('/api/deadlines', methods=['GET'])
def get_deadlines():
    deadlines = session.query(ComplianceDeadline).all()
    return jsonify([deadline.__dict__ for deadline in deadlines])

@compliance_deadline_api.route('/api/deadlines', methods=['POST'])
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
    session.add(new_deadline)
    session.commit()
    return jsonify(new_deadline.__dict__), 201

@compliance_deadline_api.route('/api/deadlines/<int:id>', methods=['PUT'])
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

    session.commit()
    return jsonify(deadline.__dict__)