import math
from typing import Dict

from src.db_model.DBAttribute import DBAttribute
from src.db_model.DBRelation import DBRelation
from src.db_model.DBTable import DBTable

from src.utility import CalcFillOrder


class DBModel:
    def __init__(self, db_tables=None):
        self.db_tables: Dict[str, DBTable] = db_tables or {}

    def add_table(self, table_name, db_table):
        self.db_tables.update({table_name: db_table})

    def generate_refinery_code(self):
        classes_strings = "\n".join([tab.generate_refinery_code() for tab in self.db_tables.values()])

        scope_strings = ",\n   ".join([f"{name}="
                                       f"{tab.scope_count_min}..{tab.scope_count_max}"
                                       for name, tab in self.db_tables.items()])

        node_multiplicity_min = sum(tab.scope_count_min for tab in self.db_tables.values())
        node_multiplicity_max = math.ceil(node_multiplicity_min * 1.2)  # TODO calc from table.scope_count_max
        scope_node_string = f"node={node_multiplicity_min}..{node_multiplicity_max},"

        return f"{classes_strings}\n\nscope {scope_node_string}\n   {scope_strings}."

    def refinery_result_to_sql(self, refinery_result_path):
        self.db_tables = CalcFillOrder.calc_fill_order(self.db_tables)
        self.refinery_result_to_db_objects(refinery_result_path)
        return self.db_objects_to_sql()

    def refinery_result_to_db_objects(self, refinery_result_path):
        with open(refinery_result_path, "r") as file:
            line = file.readline()

            # ignore lines until "declare"
            while not line.strip().startswith("declare"):
                line = file.readline()

            object_names_and_tables = {name: "" for name in line.strip("declare .\n").split(", ")}
            line = file.readline()

            # ignore lines until object creation
            while line.strip().startswith("!exists"):
                line = file.readline()

            # create objects
            while "" in object_names_and_tables.values():
                table_name, object_name = line.strip(").\n").split("(")
                self.db_tables[table_name].add_object(object_name)
                object_names_and_tables.update({object_name: table_name})
                line = file.readline()

            # create relations
            while line:
                if line.strip().startswith("default"):
                    line = file.readline()
                    continue
                origin_attribute_name, object_names = line.strip(").\n").split("::")[-1].split("(")
                origin_object_name, target_object_name = object_names.split(", ")

                origin_table_name = object_names_and_tables[origin_object_name]
                target_table_name = object_names_and_tables[target_object_name]

                target_object_id = self.db_tables[target_table_name].get_id_of(target_object_name)

                origin_object_attributes = self.db_tables[origin_table_name].objects[origin_object_name]
                origin_object_attributes.update({origin_attribute_name: target_object_id})

                line = file.readline()

    def db_objects_to_sql(self):
        insert_sql = ""

        insert_sql += "SET FOREIGN_KEY_CHECKS=0;\n"
        insert_sql += "\n".join([f"DELETE FROM {table_name};" for table_name in self.db_tables.keys()])
        insert_sql += "\nSET FOREIGN_KEY_CHECKS=1;\n"
        insert_sql += "\n\n"

        for table_name, table in self.db_tables.items():
            attribute_names = [a.name for a in table.attributes]
            insert_sql += f"INSERT INTO {table_name} ({','.join(attribute_names)}) VALUES\n"
            for object_name, db_object in table.objects.items():
                insert_sql += "(" + (",".join([db_object[attr] for attr in attribute_names])) + "),"
            insert_sql = insert_sql[:-1] + ";\n\n"

        insert_sql = insert_sql[:-1]
        return insert_sql


def generate_db_model(tables, table_relations, table_descriptions) -> DBModel:
    """
    Generate DBModel with tables, relations, and attribute based on given schema data
    """
    db_model = DBModel()

    for table in tables:
        table_name = table[0]
        db_table = DBTable(table_name)

        # Adding attributes
        attributes_for_table = table_descriptions[table_name]
        for attribute in attributes_for_table:
            db_attribute = DBAttribute(attribute[0], attribute[1], attribute[2] == "YES",
                                       attribute[3], attribute[4], attribute[5])
            db_table.add_attribute(db_attribute)

        # Adding relations
        relations_for_table = [r for r in table_relations if r[1] == table_name]
        for relation in relations_for_table:
            db_relation = DBRelation(relation[1], relation[2], relation[4], relation[5], 1, 1)
            db_table.add_relation(db_relation)

        db_model.add_table(table_name, db_table)

    return db_model
