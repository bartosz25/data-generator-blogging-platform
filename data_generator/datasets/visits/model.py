import dataclasses
import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from data_generator.datasets.data_generator_entity import DataGeneratorEntity

logger = logging.getLogger('Visit')


@dataclasses.dataclass
class TechnicalContext:
    browser: str
    browser_version: str
    network_type: str
    device_type: str
    device_version: str


@dataclasses.dataclass
class UserContext:
    ip: str
    login: Optional[str]
    connected_since: Optional[datetime]


@dataclasses.dataclass
class VisitContext:
    referral: str
    ad_id: Optional[str]
    user: UserContext
    technical: TechnicalContext


@dataclasses.dataclass
class Visit(DataGeneratorEntity):
    visit_id: str
    event_time: datetime
    user_id: str
    keep_private: bool
    page: str
    context: VisitContext

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def partition_key(self) -> str:
        # partition key can be missing due to the data quality issues
        # return a random uuid instead
        if not self.visit_id:
            return str(uuid.uuid4())
        else:
            return self.visit_id
