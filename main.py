import mysql.connector
import inquirer

from dbConfig import db_config
from dbConstructs import DBAttribute, DBRelation, DBObject
from dbModel import DBModel


def ConnectToMySQL(config):
    print(f"Connecting to db: {config['host']}:{config['port']}")
    conn = mysql.connector.connect(**config)
    print("Connected")
    return conn


def SelectDB(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    db_names = [row[0] for row in cursor.fetchall()]
    cursor.close()

    questions = [
        inquirer.List("db_name",
                      message="Select a database:",
                      choices=db_names)
    ]
    answers = inquirer.prompt(questions)
    selected_db_name = answers["db_name"]
    return selected_db_name


def GetSchemaData(conn):
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    table_descriptions = {}
    for table in tables:
        table_name = table[0]
        cursor.execute(f"DESCRIBE {table_name}")  # Field, Type, Null, Key(PRI/UNI/MUL), Default, Extra
        table_descriptions.update({table_name: cursor.fetchall()})

    cursor.execute(f"""SELECT 
                  `TABLE_SCHEMA`,`TABLE_NAME`,`COLUMN_NAME`,`REFERENCED_TABLE_SCHEMA`,`REFERENCED_TABLE_NAME`,`REFERENCED_COLUMN_NAME`
                FROM
                  `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE`
                WHERE
                  `TABLE_SCHEMA` = SCHEMA()
                  AND `REFERENCED_TABLE_NAME` IS NOT NULL;
            """)
    table_relations = cursor.fetchall()
    cursor.close()

    return tables, table_relations, table_descriptions


def GenerateDBModel(tables, table_relations, table_descriptions):
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


def CallRefinery(x_core):
    print("Calling Refinery")
    # TODO: Make HTTP call using x_core


def main():
    connection = ConnectToMySQL(db_config)
    selected_db_name = SelectDB(connection)

    connection.close()

    # Reconnect to the selected database
    db_config.update({"database": selected_db_name})
    connection = ConnectToMySQL(db_config)

    tables, table_relations, table_descriptions = GetSchemaData(connection)

    connection.close()

    db_model = GenerateDBModel(tables, table_relations, table_descriptions)
    x_core = db_model.toXCore()
    with open("out/XCore.txt", "w") as file:
        file.write(x_core)
    print("XCore code written to XCore.txt. Review it before continuing!")
    input("Press Enter to connect to Refinery...")

    # Reading back the reviewed/updated XCore
    with open("out/XCore.txt", "r") as file:
        x_core = file.read()

    # Generating data with Refinery
    CallRefinery(x_core)

    insert_sql = db_model.CSVs_to_SQL("CSVs")
    with open("out/insert_data.sql", "w") as file:
        file.write(insert_sql)
    print("Data inserting SQL written to insert_data.sql. Review it before continuing!")
    input("Press Enter to run insert_data.sql on the database...")


if __name__ == "__main__":
    main()
