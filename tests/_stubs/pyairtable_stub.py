"""Minimal pyairtable stub for testing."""
class Api:
    def __init__(self, api_key=None):
        self.api_key = api_key
    def table(self, base_id, table_name):
        return Table(base_id, table_name)

class Table:
    def __init__(self, base_id=None, table_name=None):
        self.base_id = base_id
        self.table_name = table_name
    def all(self, **kwargs): return []
    def first(self, **kwargs): return None
    def get(self, record_id): return None
    def create(self, fields): return {"id": "rec_mock", "fields": fields}
    def update(self, record_id, fields): return {"id": record_id, "fields": fields}

class Base:
    pass
