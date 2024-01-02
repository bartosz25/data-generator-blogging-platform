import datetime
from typing import Any


def json_converter(field: Any) -> str:
    if isinstance(field, datetime.datetime):
        return field.isoformat()
    else:
        return field
