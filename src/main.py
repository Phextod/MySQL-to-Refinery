import os
import subprocess

from src.config import db_config, REFINERY_CODE_PATH, REFINERY_JAR_PATH, REFINERY_RESULT_PATH, INSERT_SQL_PATH
from db_connection.DBMethods import connect_to_my_sql, select_db, get_schema_data, run_sql
from src.db_model.DBModel import generate_db_model


def run_refinery(random_seed: int = 1):
    print("Running Refinery")
    command = f"java -jar {REFINERY_JAR_PATH} generate -o {REFINERY_RESULT_PATH} -r {random_seed} {REFINERY_CODE_PATH}"
    try:
        subprocess.run(command, shell=True, check=True)
        print("Process finished successfully")
    except subprocess.CalledProcessError as e:
        print("Error executing command:", e)


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

    refinery_code = db_model.generate_refinery_code(10)

    # Create directories if they don't exist
    os.makedirs(os.path.dirname(REFINERY_CODE_PATH), exist_ok=True)
    with open(REFINERY_CODE_PATH, "w") as file:
        file.write(refinery_code)
    print(f"Refinery code written to {REFINERY_CODE_PATH}. Review it before continuing!")
    input("Press Enter to run Refinery...")

    # Generating data with Refinery
    run_refinery()

    insert_sql = db_model.refinery_result_to_sql(REFINERY_RESULT_PATH)

    with open(INSERT_SQL_PATH, "w") as file:
        file.write(insert_sql)
    print(f"Data inserting SQL written to {INSERT_SQL_PATH}. Review it before continuing!")
    input("Press Enter to run data/out/insert_data.sql on the database...")

    # Reading back the reviewed/updated sql
    with open(INSERT_SQL_PATH, "r") as file:
        insert_sql = file.read()

    connection = connect_to_my_sql(db_config)
    run_sql(connection, insert_sql)
    connection.close()


if __name__ == "__main__":
    main()
