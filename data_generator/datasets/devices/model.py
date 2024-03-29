import dataclasses
from typing import Dict, Any

from data_generator.datasets.data_generator_entity import DataGeneratorEntity


@dataclasses.dataclass
class Device(DataGeneratorEntity):
    type: str
    full_name: str
    version: str

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def entity_partition_key(self) -> str:
        return self.type
