# NotificationPreference model

class NotificationPreference:
    def __init__(self, id, entity_id, entity_type, event_type, channel, enabled, organization_id, digest_mode="IMMEDIATE", quiet_hours_start=None, quiet_hours_end=None, timezone=None):
        self.organization_id = organization_id
        self.id = id
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.event_type = event_type
        self.channel = channel
        self.enabled = enabled
        self.digest_mode = digest_mode
        self.quiet_hours_start = quiet_hours_start
        self.quiet_hours_end = quiet_hours_end
        self.timezone = timezone
        self.organization_id = organization_id
