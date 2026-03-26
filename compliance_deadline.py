from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Enum, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ComplianceDeadline(Base):
    __tablename__ = 'compliance_deadlines'

    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, nullable=False)
    entity_type = Column(String, nullable=False)
    deadline_type = Column(String, nullable=False)
    due_date = Column(DateTime, nullable=False)
    status = Column(Enum('PENDING', 'COMPLETED', 'OVERDUE', name='status_enum'), nullable=False, default='PENDING')
    organization_id = Column(Integer, nullable=False)
    metadata_json = Column(JSON, nullable=True)
    last_event_sent_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<ComplianceDeadline(id={self.id}, entity_id={self.entity_id}, entity_type={self.entity_type}, deadline_type={self.deadline_type}, due_date={self.due_date}, status={self.status})>"