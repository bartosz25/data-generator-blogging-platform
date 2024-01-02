from abc import ABC, abstractmethod
from typing import List

from data_generator.datasets.data_generator_entity import DataGeneratorEntity


class DatasetWriter(ABC):

    @abstractmethod
    def write_dataset_decorated_rows(self, decorated_rows: List[DataGeneratorEntity]): raise NotImplementedError

    @abstractmethod
    def flush(self): raise NotImplementedError
