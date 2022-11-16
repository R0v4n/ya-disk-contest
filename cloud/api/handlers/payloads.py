import json
from datetime import datetime
from functools import partial, singledispatch

from asyncpg import Record

from cloud.api.model import ItemType


@singledispatch
def convert(value):
    raise TypeError(f'Unserializable value: {value} of type: {type(value)}')


@convert.register
def convert_asyncpg_record(value: Record):
    return dict(value)


@convert.register
def convert_datetime(value: datetime):
    return str(value)


@convert.register
def convert_node_type_enum(value: ItemType):
    return value.value


dumps = partial(json.dumps, default=convert, ensure_ascii=False)
