
class DBRelation:
    def __init__(self, origin_table_name: str, origin_attribute_name: str, target_table_name: str,
                 target_attribute_name: str, multiplicity_min: int, multiplicity_max: int):
        self.origin_table_name = origin_table_name
        self.origin_attribute_name = origin_attribute_name
        self.target_table_name = target_table_name
        self.target_attribute_name = target_attribute_name

        self.multiplicity_min = multiplicity_min
        self.multiplicity_max = multiplicity_max

    def generate_refinery_code(self):
        if self.multiplicity_min == self.multiplicity_max:
            return f"    {self.target_table_name}[{self.multiplicity_min}] {self.origin_attribute_name}"
        else:
            return f"    {self.target_table_name}[{self.multiplicity_min}..{self.multiplicity_max}] {self.origin_attribute_name}"
