import mysql.connector
import inquirer
from mysql.connector import MySQLConnection


def connect_to_my_sql(config):
    print(f"Connecting to db: {config['host']}:{config['port']}")
    conn = mysql.connector.connect(**config)
    print("Connected")
    return conn


def select_db(conn):
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


def get_schema_data(conn):
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


def run_sql(conn: MySQLConnection, sql):
    print("Started running sql")
    cursor = conn.cursor()

    statements = [s.strip() for s in sql.split(";")]

    for statement in statements:
        if statement == "":
            continue

        cursor.execute(statement)
        conn.commit()

    cursor.close()
