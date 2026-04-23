"""Minimal anthropic stub for testing."""
class Anthropic:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.messages = Messages()

class Messages:
    def create(self, **kwargs):
        return MessageResponse()

class MessageResponse:
    def __init__(self):
        self.content = [ContentBlock()]

class ContentBlock:
    def __init__(self):
        self.text = '{"document_type": "COI", "named_insured": null, "policies": []}'
