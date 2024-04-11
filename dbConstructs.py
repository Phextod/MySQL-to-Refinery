import copy
from typing import List


class DBPrimitiveObject:
    def __init__(self, db_type, name):
        self.db_type = db_type
        self.name = name
        self.attributes = {}

    def update_attribute(self, attribute):
        self.attributes.update(attribute)

    def get_attribute(self, attribute):
        return self.attributes.get(attribute)


class DBAttribute:
    def __init__(self, name, db_type, nullable, default_value, key, value=None):
        self.name = name
        self.db_type = db_type
        self.nullable = nullable
        self.default_value = default_value
        self.key = key
        self.value = value


class DBRelation:
    def __init__(self, origin_table, origin_name, target_table, target_name, multiplicity_min, multiplicity_max):
        self.origin_table: str = origin_table
        self.origin_name: str = origin_name
        self.target_table: str = target_table
        self.target_name: str = target_name
        self.multiplicity_min = multiplicity_min
        self.multiplicity_max = multiplicity_max

    def toXCore(self):
        if self.multiplicity_min == self.multiplicity_max:
            return f"    {self.target_table}[{self.multiplicity_min}] {self.origin_name}"
        else:
            return f"    {self.target_table}[{self.multiplicity_min}..{self.multiplicity_max}] {self.origin_name}"


class DBObject:
    def __init__(self, name, attributes=None, relations=None, count_min=1, count_max="*"):
        self.name = name
        self.attributes: List[DBAttribute] = attributes or []
        self.relations: List[DBRelation] = relations or []  # relations pointing away from this object
        self.scope_count_min = count_min
        self.scope_count_max = count_max
        self.dependency_order = -1

    def add_attribute(self, attribute):
        self.attributes.append(attribute)

    def add_relation(self, relation):
        self.relations.append(relation)

    def toXCore(self):
        relation_strings = "\n".join([relation.toXCore() for relation in self.relations])
        if relation_strings:
            return f"class {self.name} {{\n{relation_strings}\n}}"
        else:
            return f"""class {self.name} {{}}"""

    def CSVtoDBPrimitives(self, csv_folder_path, db_primitives: List[DBPrimitiveObject]):
        """
        Stores generated DBPrimitives in the given db_primitives list
        """
        with open(f"./{csv_folder_path}/{self.name}.csv", "r") as file:
            lines = file.readlines()[1:]  # remove header line
            main_csv = [line.split(",")[0] for line in lines if not line.startswith(":")]  # remove weird ::X lines

        for i, primitive_name in enumerate(main_csv):
            db_primitive = DBPrimitiveObject(self.name, primitive_name)

            for attribute in self.attributes:
                if not attribute.nullable:
                    db_primitive.update_attribute({attribute.name: attribute.name + f"_{i}"})

            db_primitives.append(db_primitive)

        for relation in self.relations:
            with open(f"./{csv_folder_path}/{self.name}_{relation.origin_name}.csv", "r") as file:
                relation_csv = file.readlines()[1:]  # remove header line
            for line in relation_csv:
                split = line.split(",")
                origin_name = split[0]
                target_name = split[1]
                origin_primitive = [p for p in db_primitives if p.name == origin_name][0]
                target_primitive = [p for p in db_primitives if p.name == target_name][0]
                origin_primitive.update_attribute({
                    relation.origin_name: target_primitive.get_attribute(relation.target_name)
                })
