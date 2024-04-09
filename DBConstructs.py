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
        self.origin_table = origin_table
        self.origin_name = origin_name
        self.target_table = target_table
        self.target_name = target_name
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
        self.attributes = attributes or []
        self.relations = relations or []
        self.scope_count_min = count_min
        self.scope_count_max = count_max

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


class DBModel:
    def __init__(self, db_object=None):
        self.db_objects = db_object or []
        self.db_primitives = []

    def add_db_object(self, db_object):
        self.db_objects.append(db_object)

    def toXCore(self):
        xcore_classes_strings = "\n".join([db_objects.toXCore() for db_objects in self.db_objects])
        scope_strings = ",\n   ".join([f"{db_objects.name}="
                                       f"{db_objects.scope_count_min}..{db_objects.scope_count_max}"
                                       for db_objects in self.db_objects])
        return f"{xcore_classes_strings}\n\nscope\n   {scope_strings}."

    def CSVtoDBPrimitives(self, csv_folder_path):
        # Determine which tables to fill based on dependencies
        unfilled_db_objects = copy.deepcopy(self.db_objects)
        while unfilled_db_objects:
            for db_object in unfilled_db_objects:
                for db_relation in db_object.relations:
                    # if foreign key table is not yet filled
                    if db_relation.target_table in [o.name for o in unfilled_db_objects] \
                            and db_relation.target_table != db_object.name:
                        break
                else:
                    # here db_object has no unsatisfied relations, and can be processed safely
                    db_object.CSVtoDBPrimitives(csv_folder_path, self.db_primitives)
                    unfilled_db_objects.remove(db_object)
                    continue

    def DBPrimitivesToSQL(self):

        print("DONE")
