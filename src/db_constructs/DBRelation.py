from typing import Optional

from src.db_constructs.DBAttribute import DBAttribute


class DBRelation:
    def __init__(self, origin_table, origin_name, target_table, target_name, multiplicity_min, multiplicity_max):
        self.origin_table: str = origin_table
        self.origin_name: str = origin_name
        self.target_table: str = target_table
        self.target_name: str = target_name
        self.multiplicity_min = multiplicity_min
        self.multiplicity_max = multiplicity_max
        self.target_attribute: Optional[DBAttribute] = None

    def to_x_core(self):
        if self.multiplicity_min == self.multiplicity_max:
            return f"    {self.target_table}[{self.multiplicity_min}] {self.origin_name}"
        else:
            return f"    {self.target_table}[{self.multiplicity_min}..{self.multiplicity_max}] {self.origin_name}"
