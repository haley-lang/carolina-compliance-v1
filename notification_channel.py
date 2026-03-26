# NotificationChannel interface/base

class NotificationChannel:
    def send(self, recipient, template_id, payload):
        raise NotImplementedError("Send method must be implemented by subclasses.")