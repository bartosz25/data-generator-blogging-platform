import dataclasses
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from data_generator.datasets.data_generator_entity import DataGeneratorEntity
from data_generator.io.converters import datetime_to_str

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
        visit_dict = dataclasses.asdict(self)
        # This field is nested and CSV writer simply stringifies the whole nested dictionary
        # instead of applying str() on each entry. That's why, we're doing here the explicit date-to-string formatting
        # For exactly the same reason we remove the entry. Otherwise, it would produce '"connected_since": None'
        if self.context and self.context.user:
            if self.context.user.connected_since:
                visit_dict['context']['user']['connected_since'] = datetime_to_str(
                    visit_dict['context']['user']['connected_since'])
            else:
                del visit_dict['context']['user']['connected_since']
        return visit_dict

    def entity_partition_key(self) -> str:
        return self.visit_id
