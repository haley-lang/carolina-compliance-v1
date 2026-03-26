# EmailNotificationChannel adapter

from notification_channel import NotificationChannel

class EmailNotificationChannel(NotificationChannel):
    def __init__(self, sendgrid_service):
        self.sendgrid_service = sendgrid_service

    def send(self, recipient, template_id, payload):
        self.sendgrid_service.send_email(template_id, payload)