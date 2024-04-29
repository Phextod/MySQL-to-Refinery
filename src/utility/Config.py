# MySQL connection configuration
import json
import os.path


class Config:
    def __init__(self, config_file_path):
        with open(config_file_path) as config_file:
            config = json.load(config_file)

            self.db_config = config['db_config']
            self.REFINERY_JAR_PATH = os.path.abspath(config['REFINERY_JAR_PATH'])
            self.REFINERY_CODE_PATH = os.path.abspath(config['REFINERY_CODE_PATH'])
            self.REFINERY_RESULT_PATH = os.path.abspath(config['REFINERY_RESULT_PATH'])
            self.INSERT_SQL_PATH = os.path.abspath(config['INSERT_SQL_PATH'])
            self.BENCHMARK_RESULT_PATH = os.path.abspath(config['BENCHMARK_RESULT_PATH'])
            self.FULL_BENCHMARK_RESULT_PATH = os.path.abspath(config['FULL_BENCHMARK_RESULT_PATH'])


def load_config(config_file_path):
    return Config(config_file_path)
