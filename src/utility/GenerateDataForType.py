from faker import Faker

fake = Faker()


def generate_data_for_type(db_type, seed, object_name="") -> str:
    Faker.seed(seed)

    # https://dev.mysql.com/doc/refman/8.3/en/integer-types.html
    if 'tinyint' == db_type or 'smallint' == db_type or 'mediumint' == db_type \
            or 'int' == db_type or 'bigint' == db_type:
        return str(seed % 127)

    elif 'float' == db_type:
        return str(fake.pyfloat())

    elif 'double' == db_type:
        return str(fake.pyfloat())

    # https://dev.mysql.com/doc/refman/8.3/en/precision-math-decimal-characteristics.html
    elif 'decimal' in db_type:
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

        decimal_value = fake.random_number(digits=precision - scale, fix_len=False) + \
                        fake.random_number(digits=scale, fix_len=False) * 10 ** -scale

        return str(decimal_value)

    elif 'char' == db_type:
        n = (seed % 52) + ord("A")
        if n > ord("Z"):
            n += ord("a") - ord("Z") - 1
        return chr(n)

    elif 'text' == db_type:
        return f"'{seed}_{object_name}_text'"

    elif 'varchar' in db_type:
        length = int(db_type.split('(')[1].split(')')[0])
        text = f"{seed}_{object_name}_varchar"[:length]
        return f"'{text}'"

    elif 'date' == db_type or 'time' == db_type or 'timestamp' == db_type:
        return f"'{fake.date_time_this_century()}'"

    else:
        return "NULL"
