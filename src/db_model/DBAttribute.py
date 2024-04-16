class DBAttribute:
    def __init__(self, name: str, db_type: str, nullable: bool, key_type: str, default_value, extra: str):
        self.name = name
        self.db_type = db_type
        self.nullable = nullable
        self.key_type = key_type  # PRI/UNI/MUL
        self.default_value = default_value
        self.extra = extra
