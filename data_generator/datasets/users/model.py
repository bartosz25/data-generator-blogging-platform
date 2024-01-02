import dataclasses
import datetime
from typing import Optional, Dict, Any

from data_generator.datasets.data_generator_entity import DataGeneratorEntity


@dataclasses.dataclass
class RegisteredUser(DataGeneratorEntity):
    id: str
    login: str
    email: str
    registered_datetime: datetime.datetime
    first_connection_datetime: Optional[datetime.datetime]
    last_connection_datetime: Optional[datetime.datetime]

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def partition_key(self) -> str:
        return self.login

