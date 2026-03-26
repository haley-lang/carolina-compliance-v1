# NotificationRule model

RULE_STORE = []

class NotificationRule:
    def __init__(self, id, name, event_type, condition_json, template_id, enabled, organization_id, required_variables=None, escalation_delay_days=None, escalation_recipient=None):
        self.organization_id = organization_id
        self.required_variables = required_variables or []
        self.escalation_delay_days = escalation_delay_days
        self.organization_id = organization_id
        self.escalation_recipient = escalation_recipient
        self.id = id
        self.name = name
        self.event_type = event_type
        self.condition_json = condition_json
        self.template_id = template_id
        self.enabled = enabled
        self.escalation_delay_days = escalation_delay_days
        self.escalation_recipient = escalation_recipient
        RULE_STORE.append(self)

    @staticmethod
    def get_all():
        return RULE_STORE
