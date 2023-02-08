import json
from datetime import datetime
from functools import partial, singledispatch
from typing import Any

from aiohttp.payload import JsonPayload as BaseJsonPayload
from aiohttp.typedefs import JSONEncoder
from asyncpg import Record

from cloud.db.schema import ItemType


@singledispatch
def convert(value):
    raise TypeError(f'Unserializable value: {value} of type: {type(value)}')


@convert.register
def convert_asyncpg_record(value: Record):
    return dict(value)


@convert.register
def convert_datetime(value: datetime):
    return value.isoformat()


@convert.register
def convert_node_type_enum(value: ItemType):
    return value.value


dumps = partial(json.dumps, default=convert, ensure_ascii=False)


class JsonPayload(BaseJsonPayload):

    def __init__(self,
                 value: Any,
                 encoding: str = 'utf-8',
                 content_type: str = 'application/json',
                 dumps: JSONEncoder = dumps,
                 *args: Any,
                 **kwargs: Any) -> None:
        super().__init__(value, encoding, content_type, dumps, *args, **kwargs)


__all__ = ('JsonPayload',)
