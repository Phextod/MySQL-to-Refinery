from src.utility.data_generators.DataGeneratorDefault import DataGeneratorDefault


def generate_data_for_type(db_type: str, seed: int, object_name: str = "", is_id=False) -> str:
    return DataGeneratorDefault(seed).generate(db_type, object_name, is_id)
