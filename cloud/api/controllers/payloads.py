import json
from datetime import datetime
from functools import partial

from asyncpg import Record

from cloud.api.model import NodeType


# todo: refactor singledispathed
def convert_asyncpg_record(value):
    """
    Позволяет автоматически сериализовать результаты запроса, возвращаемые
    asyncpg.
    """
    if type(value) == Record:
        return dict(value)

    if type(value) == datetime:
        return str(value)

    if type(value) == NodeType:
        return value.value

    raise TypeError(f'Unserializable value: {value} of type: {type(value)}')


dumps = partial(json.dumps, default=convert_asyncpg_record, ensure_ascii=False)
