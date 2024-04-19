import argparse
import os

from src.db_connection.DBMethods import connect_to_my_sql, select_db, get_schema_data, run_sql
from src.db_model.DBModel import generate_db_model
from src.utility.Config import load_config
from src.utility.run_refinery import run_refinery


def main():
    # Load config
    config = load_config("config.json")
    parse_arguments(config)

    # Connect and select target db
    connection = connect_to_my_sql(config.db_config)
    selected_db_name = select_db(connection)
    connection.close()

    # Reconnect to the selected database
    config.db_config.update({"database": selected_db_name})
    connection = connect_to_my_sql(config.db_config)

    # Get schema data
    tables, table_relations, table_descriptions = get_schema_data(connection)
    connection.close()

    # Generate refinery code
    db_model = generate_db_model(tables, table_relations, table_descriptions)
    refinery_code = db_model.generate_refinery_code(10)

    # Create directories if they don't exist
    os.makedirs(os.path.dirname(config.REFINERY_CODE_PATH), exist_ok=True)
    with open(config.REFINERY_CODE_PATH, "w") as file:
        file.write(refinery_code)
    print(f"Refinery code written to {config.REFINERY_CODE_PATH}. Review it before continuing!")
    input("Press Enter to run Refinery...")

    # Generating data with Refinery
    run_refinery(config.REFINERY_JAR_PATH, config.REFINERY_CODE_PATH, config.REFINERY_RESULT_PATH)

    # Generate insert sql
    insert_sql = db_model.refinery_result_to_sql(config.REFINERY_RESULT_PATH)

    with open(config.INSERT_SQL_PATH, "w") as file:
        file.write(insert_sql)
    print(f"Data inserting SQL written to {config.INSERT_SQL_PATH}. Review it before continuing!")
    input("Press Enter to run data/out/insert_data.sql on the database...")

    # Reading back the reviewed/updated sql
    with open(config.INSERT_SQL_PATH, "r") as file:
        insert_sql = file.read()

    # Run insert sql
    connection = connect_to_my_sql(config.db_config)
    run_sql(connection, insert_sql)
    connection.close()


def parse_arguments(config):
    parser = argparse.ArgumentParser()
    parser.parse_args()


if __name__ == "__main__":
    main()
