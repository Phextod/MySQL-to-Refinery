import argparse
import os

from src.db_connection.DBMethods import connect_to_my_sql, select_db, get_schema_data, run_sql
from src.db_model.DBModel import generate_db_model
from src.utility.Config import load_config
from src.utility.run_refinery import run_refinery


def main():
    args = parse_arguments()

    # Load config
    config = load_config(args.config)

    selected_db_name = args.db_name
    if not selected_db_name:
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
    refinery_code = db_model.generate_refinery_code(args.node_min_multi, args.node_max_multi)

    # Create directories if they don't exist
    os.makedirs(os.path.dirname(config.REFINERY_CODE_PATH), exist_ok=True)

    # Save refinery code
    with open(config.REFINERY_CODE_PATH, "w") as file:
        file.write(refinery_code)

    if not args.skip_refinery_review:
        print(f"Refinery code written to {config.REFINERY_CODE_PATH}. Review it before continuing!")
        input("Press Enter to run Refinery...")

    # Generating data with Refinery
    run_refinery(config.REFINERY_JAR_PATH, config.REFINERY_CODE_PATH, config.REFINERY_RESULT_PATH)

    # Generate and save insert sql
    insert_sql = db_model.refinery_result_to_sql(config.REFINERY_RESULT_PATH)
    with open(config.INSERT_SQL_PATH, "w") as file:
        file.write(insert_sql)

    if not args.skip_sql_review:
        print(f"Data inserting SQL written to {config.INSERT_SQL_PATH}. Review it before continuing!")
        input("Press Enter to run data/out/insert_data.sql on the database...")

        # Reading back the reviewed/updated sql
        with open(config.INSERT_SQL_PATH, "r") as file:
            insert_sql = file.read()

    if not args.skip_sql_insert:
        # Run insert sql
        connection = connect_to_my_sql(config.db_config)
        run_sql(connection, insert_sql)
        connection.close()


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config",
                        default="config.json",
                        help="Path to a custom configuration JSON file")
    parser.add_argument("-db", "--db-name",
                        help="Name of the database to be generated")
    parser.add_argument("-min", "--node-min-multi",
                        default=10,
                        type=float,
                        help="Multiplier for the minimum node count in refinery. (default: 10)")
    parser.add_argument("-max", "--node-max-multi",
                        default=1.2,
                        type=float,
                        help="Multiplier for the maximum node count in refinery. max = min * max_multi. (default: 1.2)")
    parser.add_argument("-srr", "--skip-refinery-review",
                        action="store_true",
                        help="Do not stop execution to review refinery code")
    parser.add_argument("-ssr", "--skip-sql-review",
                        action="store_true",
                        help="Do not stop execution to review SQL code")
    parser.add_argument("-ssi", "--skip-sql-insert",
                        action="store_true",
                        help="Do not run insert SQL on db")

    return parser.parse_args()


if __name__ == "__main__":
    main()
