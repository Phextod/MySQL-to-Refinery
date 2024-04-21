from abc import abstractmethod, ABC


class AbsDataGenerator(ABC):

    def __init__(self, seed: int):
        """
        :param seed: seed value is unique for each object in a table
        """
        self.seed = seed

    @abstractmethod
    def generate(self, db_type: str, object_name: str, is_id) -> str:
        return "NULL"

