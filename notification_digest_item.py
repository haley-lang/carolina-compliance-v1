from sqlalchemy import Column, Integer, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class NotificationDigestItem(Base):
    __tablename__ = 'notification_digest_items'

    id = Column(Integer, primary_key=True)
    recipient = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    payload_json = Column(JSON, nullable=False)
    template_id = Column(String, nullable=False)
    organization_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default='QUEUED_DIGEST')
    pending_send_at = Column(DateTime, nullable=True)  # New field for quiet hours