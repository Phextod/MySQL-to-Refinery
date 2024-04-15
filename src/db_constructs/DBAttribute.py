class DBAttribute:
    def __init__(self, name, db_type, nullable, default_value, key, extra, value=None):
        self.name = name
        self.db_type = db_type
        self.nullable = nullable
        self.default_value = default_value
        self.key = key
        self.extra = extra
        self.value = value
