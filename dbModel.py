from typing import List
from dbConstructs import DBObject, DBPrimitiveObject

import calcFillOrder


class DBModel:
    def __init__(self, db_object=None):
        self.db_objects: List[DBObject] = db_object or []
        self.db_primitives: List[DBPrimitiveObject] = []

    def add_db_object(self, db_object):
        self.db_objects.append(db_object)

    def toXCore(self):
        xcore_classes_strings = "\n".join([db_objects.toXCore() for db_objects in self.db_objects])
        scope_strings = ",\n   ".join([f"{db_objects.name}="
                                       f"{db_objects.scope_count_min}..{db_objects.scope_count_max}"
                                       for db_objects in self.db_objects])
        return f"{xcore_classes_strings}\n\nscope\n   {scope_strings}."

    def CSVs_to_DBPrimitives(self, csv_folder_path, db_objects_in_fill_order):
        print("csvs to primitives")

    def DBPrimitives_to_SQL(self):
        insert_sql = ""

        insert_sql += "\n".join([f"DELETE FROM {o.name}" for o in self.db_objects])
        insert_sql += "\n\n"

        for table in self.db_objects:
            insert_sql += f"insert into {table.name}({','.join([a.name for a in table.attributes])}) values\n"

            # for attribute in table.attributes:

            insert_sql += "\n"

        return insert_sql

    def CSVs_to_SQL(self, csv_folder_path):
        ordered_list = calcFillOrder.calc_fill_order(self.db_objects)
        self.CSVs_to_DBPrimitives(csv_folder_path, ordered_list)
        return self.DBPrimitives_to_SQL()
