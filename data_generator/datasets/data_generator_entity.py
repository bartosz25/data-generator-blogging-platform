from abc import ABC, abstractmethod
from typing import Dict, Any


class DataGeneratorEntity(ABC):
    @abstractmethod
    def as_dict(self) -> Dict[str, Any]: raise NotImplementedError

    @abstractmethod
    def partition_key(self) -> str: raise NotImplementedError


class EntityGenerator(ABC):
    @abstractmethod
    def generate_row(self, index: int) -> DataGeneratorEntity: raise NotImplementedError
