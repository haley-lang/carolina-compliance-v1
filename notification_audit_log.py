from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Enum, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class NotificationAuditLog(Base):
    __tablename__ = 'notification_audit_logs'

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, nullable=False)
    event_type = Column(String, nullable=False)
    recipient = Column(String, nullable=False)
    template_id = Column(String, nullable=False)
    status = Column(Enum('SUCCESS', 'FAILED', 'SKIPPED_DUPLICATE', 'SKIPPED_INVALID_PAYLOAD', name='status_enum'), nullable=False)
    organization_id = Column(Integer, nullable=False)
    retry_count = Column(Integer, default=0)
    next_retry_at = Column(DateTime, nullable=True)
    sent_at = Column(DateTime, default=func.now())
    error_message = Column(String, nullable=True)

    def __repr__(self):
        return f"<NotificationAuditLog(id={self.id}, rule_id={self.rule_id}, event_type={self.event_type}, recipient={self.recipient}, template_id={self.template_id}, status={self.status})>"