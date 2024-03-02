import datetime
from typing import Any


def json_converter(field: Any) -> str:
    if isinstance(field, datetime.datetime):
        return datetime_to_str(field)
    else:
        return field


def datetime_to_str(field: datetime.datetime) -> str:
    return field.isoformat()
