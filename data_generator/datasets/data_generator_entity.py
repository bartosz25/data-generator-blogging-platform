import uuid
from abc import ABC, abstractmethod
from typing import Dict, Any


class DataGeneratorEntity(ABC):
    @abstractmethod
    def as_dict(self) -> Dict[str, Any]: raise NotImplementedError

    @abstractmethod
    def entity_partition_key(self) -> str: raise NotImplementedError

    def partition_key(self) -> str:
        # partition key can be missing due to the data quality issues
        # return a random uuid instead
        entity_partition_key = self.entity_partition_key()
        if not entity_partition_key:
            return str(uuid.uuid4())
        else:
            return entity_partition_key


class EntityGenerator(ABC):
    @abstractmethod
    def generate_row(self, index: int) -> DataGeneratorEntity: raise NotImplementedError
