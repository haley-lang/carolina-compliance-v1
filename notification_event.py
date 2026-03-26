# NotificationEvent structure

class NotificationEvent:
    def __init__(self, event_type, payload, timestamp):
        self.type = event_type
        self.payload = payload
        self.timestamp = timestamp