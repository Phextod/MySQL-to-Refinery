from db_connection.DBConfig import db_config
from db_connection.DBMethods import connect_to_my_sql, select_db, get_schema_data
from db_constructs.DBObject import DBObject
from db_constructs.DBRelation import DBRelation
from db_constructs.DBAttribute import DBAttribute
from db_constructs.DBModel import DBModel


def generate_db_model(tables, table_relations, table_descriptions):
    db_model = DBModel()

    for table in tables:
        table_name = table[0]
        db_object = DBObject(table_name)

        # Adding attributes
        relevant_attributes = table_descriptions[table_name]
        for attribute in relevant_attributes:
            db_attribute = DBAttribute(attribute[0], attribute[1], attribute[2] == "YES",
                                       attribute[3], attribute[4], attribute[5])
            db_object.add_attribute(db_attribute)

        # Adding relations
        relevant_relations = [r for r in table_relations if r[1] == table_name]
        for relation in relevant_relations:
            db_relation = DBRelation(relation[1], relation[2], relation[4], relation[5], 1, 1)
            db_object.add_relation(db_relation)
        db_model.add_db_object(db_object)

    db_model.update_relations_by_attributes()

    return db_model


def call_refinery(x_core):
    print("Calling Refinery")
    # TODO: Make HTTP call using x_core


def main():
    connection = connect_to_my_sql(db_config)
    selected_db_name = select_db(connection)
    connection.close()

    # Reconnect to the selected database
    db_config.update({"database": selected_db_name})
    connection = connect_to_my_sql(db_config)

    tables, table_relations, table_descriptions = get_schema_data(connection)

    connection.close()

    db_model = generate_db_model(tables, table_relations, table_descriptions)
    x_core = db_model.to_x_core()
    with open("./data/out/XCore.txt", "w") as file:
        file.write(x_core)
    print("XCore code written to XCore.txt. Review it before continuing!")
    input("Press Enter to connect to Refinery...")

    # Reading back the reviewed/updated XCore
    with open("data/out/XCore.txt", "r") as file:
        x_core = file.read()

    # Generating data with Refinery
    call_refinery(x_core)

    insert_sql = db_model.csvs_to_sql("data/in/CSVs")
    with open("data/out/insert_data.sql", "w") as file:
        file.write(insert_sql)
    print("Data inserting SQL written to insert_data.sql. Review it before continuing!")
    input("Press Enter to run insert_data.sql on the database...")


if __name__ == "__main__":
    main()
