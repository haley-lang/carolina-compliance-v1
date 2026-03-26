from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class NotificationTemplate(Base):
    __tablename__ = 'notification_templates'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    body = Column(String, nullable=False)
    organization_id = Column(Integer, nullable=False)
    required_variables = Column(String)

    @staticmethod
    def create_weekly_summary_template(session):
        # Check if the template already exists
        existing_template = session.query(NotificationTemplate).filter_by(name='Weekly Summary').first()
        if existing_template:
            return

        # Create a new template
        weekly_summary_template = NotificationTemplate(
            name='Weekly Summary',
            subject='Weekly Compliance Summary',
            body='Upcoming Deadlines: {upcoming_deadlines}\n'
                 'Overdue Deadlines: {overdue_deadlines}\n'
                 'Failed Notifications: {failed_notifications}',
            organization_id=0,  # Assuming a default organization ID
            required_variables='upcoming_deadlines,overdue_deadlines,failed_notifications'
        )
        session.add(weekly_summary_template)
        session.commit()

# Example engine creation, replace with actual engine in use
engine = create_engine('sqlite:///notifications.db')
Base.metadata.create_all(engine)