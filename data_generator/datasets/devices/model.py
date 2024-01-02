import dataclasses
from typing import Dict, Any, List

from data_generator.datasets.data_generator_entity import DataGeneratorEntity


@dataclasses.dataclass
class Device(DataGeneratorEntity):
    type: str
    full_name: str
    version: str

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def partition_key(self) -> str:
        return self.type
