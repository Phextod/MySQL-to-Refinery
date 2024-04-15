class DBPrimitiveObject:
    def __init__(self, db_type, name):
        self.db_type = db_type
        self.name = name
        self.attributes = {}

    def update_attribute(self, attribute):
        self.attributes.update(attribute)

    def get_attribute(self, attribute):
        return self.attributes.get(attribute)