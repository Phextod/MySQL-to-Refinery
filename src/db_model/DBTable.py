from typing import List, Dict
from enum import Enum

from src.db_model.DBAttribute import DBAttribute
from src.db_model.DBRelation import DBRelation
from src.utility.GenerateDataForType import generate_data_for_type


class FillStatus(Enum):
    NOT_FILLED = 1
    HALF_FILLED = 2
    FILLED = 3


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

        # Attributes for filling with data at sql generation
        self.fill_status = FillStatus.NOT_FILLED

    def add_attribute(self, attribute):
        self.attributes.append(attribute)

    def add_relation(self, relation):
        self.relations.append(relation)

    def add_object(self, object_name: str):
        generation_seed = int(object_name.lower().replace(self.name.lower(), ""))
        attribute_values = {}
        reference_attribute_names = [r.origin_attribute_name for r in self.relations]
        for attribute in self.attributes:
            attribute_value = "NULL"
            if attribute.name not in reference_attribute_names:
                is_id = attribute.key_type == "PRI"
                attribute_value = generate_data_for_type(attribute.db_type, generation_seed, object_name, is_id)
            attribute_values.update({
                attribute.name: attribute_value
            })
        self.objects.update({object_name: {name: val for name, val in attribute_values.items()}})

    def get_id_of(self, object_name):
        id_attribute_name = [a.name for a in self.attributes if a.key_type == "PRI"][0]
        return self.objects[object_name][id_attribute_name]
