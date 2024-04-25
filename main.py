import argparse
import os
import time

from src.db_connection.DBMethods import connect_to_my_sql, select_db, get_schema_data, run_sql
from src.db_model.DBModel import generate_db_model
from src.utility.Config import load_config
from src.utility.run_refinery import run_refinery


def main():
    args = parse_arguments()
    config = load_config(args.config)

    benchmark_times = {}
    benchmark_times.update({"start": time.time()})  # benchmarking

    db_model = db_to_dbmodel(args, config)
    benchmark_times.update({"db_model_done": time.time()})  # benchmarking

    dbmodel_to_refinery_code(args, config, db_model)
    benchmark_times.update({"refinery_code_done": time.time()})  # benchmarking

    run_refinery(config.REFINERY_JAR_PATH, config.REFINERY_CODE_PATH, config.REFINERY_RESULT_PATH)
    benchmark_times.update({"refinery_run_done": time.time()})  # benchmarking

    insert_sql = db_model.refinery_result_to_sql(config.REFINERY_RESULT_PATH)
    benchmark_times.update({"insert_sql_done": time.time()})  # benchmarking

    insert_sql_to_db(args, config, insert_sql)
    benchmark_times.update({"insert_sql_run_done": time.time()})  # benchmarking

    save_benchmark_times(args, config, benchmark_times)


def save_benchmark_times(args, config, benchmark_times):
    if not args.benchmark:
        return

    with open(config.BENCHMARK_RESULT_PATH, "w") as file:
        prev_timestamp = benchmark_times["start"]
        for benchmark_name, timestamp in benchmark_times.items():
            file.write(f"{benchmark_name}: {timestamp - prev_timestamp}\n")
            prev_timestamp = timestamp


def db_to_dbmodel(args, config):
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

    db_model = generate_db_model(tables, table_relations, table_descriptions)
    return db_model


def dbmodel_to_refinery_code(args, config, db_model):
    refinery_code = db_model.generate_refinery_code(args.node_min_multi, args.node_max_multi)

    # Create directories if they don't exist
    os.makedirs(os.path.dirname(config.REFINERY_CODE_PATH), exist_ok=True)

    # Save refinery code
    with open(config.REFINERY_CODE_PATH, "w") as file:
        file.write(refinery_code)

    if not args.skip_refinery_review:
        print(f"Refinery code written to {config.REFINERY_CODE_PATH}. Review it before continuing!")
        input("Press Enter to run Refinery...")


def insert_sql_to_db(args, config, insert_sql):
    with open(config.INSERT_SQL_PATH, "w") as file:
        file.write(insert_sql)

    if not args.skip_sql_review:
        print(f"Data inserting SQL written to {config.INSERT_SQL_PATH}. Review it before continuing!")
        input(f"Press Enter to run {config.INSERT_SQL_PATH} on the database...")

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
    parser.add_argument("-b", "--benchmark",
                        action="store_true",
                        help="Save benchmark times to a file")

    return parser.parse_args()


if __name__ == "__main__":
    main()
