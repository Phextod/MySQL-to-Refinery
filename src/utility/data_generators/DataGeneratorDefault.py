from abc import abstractmethod, ABC

from faker import Faker

from src.utility.data_generators.AbsDataGenerator import AbsDataGenerator


class DataGeneratorDefault(AbsDataGenerator):
    fake = Faker()

    def __init__(self, seed: int):
        """
        :param seed: seed value is unique for each object in a table
        """
        Faker.seed(seed)
        super().__init__(seed)

    def generate(self, db_type: str, object_name: str, is_id) -> str:
        if db_type.startswith("int"):
            return self.generate_int(db_type, object_name, is_id)
        elif db_type.startswith("tinyint"):
            return self.generate_tinyint(db_type, object_name, is_id)
        elif db_type.startswith("smallint"):
            return self.generate_smallint(db_type, object_name, is_id)
        elif db_type.startswith("bigint"):
            return self.generate_bigint(db_type, object_name, is_id)
        elif db_type.startswith("float"):
            return self.generate_float(db_type, object_name, is_id)
        elif db_type.startswith("double"):
            return self.generate_double(db_type, object_name, is_id)
        elif db_type.startswith("decimal"):
            return self.generate_decimal(db_type, object_name, is_id)
        elif db_type.startswith("char"):
            return self.generate_char(db_type, object_name, is_id)
        elif db_type.startswith("varchar"):
            return self.generate_varchar(db_type, object_name, is_id)
        elif db_type == "text":
            return self.generate_text(db_type, object_name, is_id)
        elif db_type == "date":
            return self.generate_date(db_type, object_name, is_id)
        else:
            return "NULL"

    def generate_int(self, db_type: str, object_name: str, is_id) -> str:
        # https://dev.mysql.com/doc/refman/8.3/en/integer-types.html
        max_unsigned = 4294967295
        val = self.seed % (max_unsigned + 1)
        if "unsigned" not in db_type:
            if val > max_unsigned // 2:
                val -= (max_unsigned // 2) * 2 + 1
        return str(val)

    def generate_tinyint(self, db_type: str, object_name: str, is_id) -> str:
        # https://dev.mysql.com/doc/refman/8.3/en/integer-types.html
        max_unsigned = 255
        val = self.seed % (max_unsigned + 1)
        if "unsigned" not in db_type:
            if val > max_unsigned // 2:
                val -= (max_unsigned // 2) * 2 + 1
        return str(val)

    def generate_smallint(self, db_type: str, object_name: str, is_id) -> str:
        # https://dev.mysql.com/doc/refman/8.3/en/integer-types.html
        max_unsigned = 65535
        val = self.seed % (max_unsigned + 1)
        if "unsigned" not in db_type:
            if val > max_unsigned // 2:
                val -= (max_unsigned // 2) * 2 + 1
        return str(val)

    def generate_bigint(self, db_type: str, object_name: str, is_id) -> str:
        # https://dev.mysql.com/doc/refman/8.3/en/integer-types.html
        max_unsigned = 2 ** 64 - 1
        val = self.seed % (max_unsigned + 1)
        if "unsigned" not in db_type:
            if val > max_unsigned // 2:
                val -= (max_unsigned // 2) * 2 + 1
        return str(val)

    def generate_float(self, db_type: str, object_name: str, is_id) -> str:
        return str(self.fake.pyfloat())

    def generate_double(self, db_type: str, object_name: str, is_id) -> str:
        return str(self.fake.pyfloat())

    def generate_decimal(self, db_type: str, object_name: str, is_id) -> str:
        # https://dev.mysql.com/doc/refman/8.3/en/precision-math-decimal-characteristics.html
        precision = 10
        scale = 0

        if '(' in db_type:
            start_index = db_type.index('(') + 1
            end_index = db_type.index(')')
            precision_scale = db_type[start_index:end_index]

            if ',' in precision_scale:
                precision, scale = map(int, precision_scale.split(','))
            else:
                precision = int(precision_scale)

        scale = min(scale, precision)

        decimal_value = self.fake.random_number(digits=precision - scale, fix_len=False) + \
                        self.fake.random_number(digits=scale, fix_len=False) * 10 ** -scale

        return str(decimal_value)

    def generate_char(self, db_type: str, object_name: str, is_id) -> str:
        # https://dev.mysql.com/doc/refman/8.3/en/char.html
        length = 1
        if "(" in db_type:
            length = db_type.split("(")[1].split(")")[0]
        result = ""
        for i in range(length):
            char_num = (self.seed % 52) + ord("A")
            if char_num > ord("Z"):
                char_num += ord("a") - ord("Z") - 1
            result += chr(char_num)
        return result

    def generate_varchar(self, db_type: str, object_name: str, is_id) -> str:
        # https://dev.mysql.com/doc/refman/8.3/en/char.html
        length = int(db_type.split('(')[1].split(')')[0])
        text = f"{self.seed}_{object_name}_varchar"[:length]
        return f"'{text}'"

    def generate_text(self, db_type: str, object_name: str, is_id) -> str:
        return f"'{self.seed}_{object_name}_text'"

    def generate_date(self, db_type: str, object_name: str, is_id) -> str:
        return f"'{self.fake.date_time_this_century()}'"
