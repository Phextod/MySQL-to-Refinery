# MySQL connection configuration
import os.path

db_config = {
    "user": "root",
    "password": "rootpwd",
    "host": "localhost",
    "port": "32769"
}

# paths are relative to config.py
REFINERY_JAR_PATH = os.path.abspath("../refinery_generator/refinery-generator-cli-0.0.0-SNAPSHOT-all.jar")
REFINERY_CODE_PATH = os.path.abspath("../data/out/refinery_code.problem")
REFINERY_RESULT_PATH = os.path.abspath("../data/out/refinery_result.txt")
INSERT_SQL_PATH = os.path.abspath("../data/out/insert_data.sql")
