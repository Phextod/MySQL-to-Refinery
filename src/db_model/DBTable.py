from typing import List, Dict

from src.db_model.DBAttribute import DBAttribute
from src.db_model.DBRelation import DBRelation
from src.utility.GenerateDataForType import generate_data_for_type


class DBTable:
    def __init__(self, name, attributes=None, relations=None, count_min=1, count_max="*"):
        self.name = name
        self.attributes: List[DBAttribute] = attributes or []
        self.relations: List[DBRelation] = relations or []  # relations pointing away from this object
        self.objects: Dict[str, Dict[str, str]] = {}
        # objects format example: {name1: {attribute1: val1, attribute2: val2...}, {name2: {attr...}...}

        # Attributes for refinery
        self.scope_count_min = count_min
        self.scope_count_max = count_max

    def add_attribute(self, attribute):
        self.attributes.append(attribute)

    def add_relation(self, relation):
        self.relations.append(relation)

    def add_object(self, object_name):
        generation_seed = int(object_name.replace(self.name, ""))
        attribute_values = {}
        for attribute in self.attributes:
            attribute_values.update({
                attribute.name: generate_data_for_type(attribute.db_type, generation_seed, object_name)
            })
        self.objects.update({object_name: {name: val for name, val in attribute_values.items()}})

    def get_id_of(self, object_name):
        # todo deal with composite keys?
        id_attribute_name = [a.name for a in self.attributes if a.key_type == "PRI"][0]
        return self.objects[object_name][id_attribute_name]

    def generate_refinery_code(self):
        relation_strings = "\n".join([relation.generate_refinery_code() for relation in self.relations])
        if relation_strings:
            return f"class {self.name} {{\n{relation_strings}\n}}"
        else:
            return f"""class {self.name} {{}}"""
