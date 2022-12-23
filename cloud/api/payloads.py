import json
from collections import deque
from datetime import datetime
from functools import partial, singledispatch
from typing import Any

from aiohttp.typedefs import JSONEncoder
from asyncpg import Record
from aiohttp.payload import JsonPayload as BaseJsonPayload, Payload

from cloud.api.model import ItemType


@singledispatch
def convert(value):
    raise TypeError(f'Unserializable value: {value} of type: {type(value)}')


@convert.register
def convert_asyncpg_record(value: Record):
    return dict(value)


@convert.register
def convert_datetime(value: datetime):
    return value.isoformat(sep=' ')


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


class JsonNodeTreeAsyncGenPayload(Payload):
    def __init__(self, value, encoding: str = 'utf-8',
                 content_type: str = 'application/json',
                 *args, **kwargs):
        super().__init__(value, content_type=content_type, encoding=encoding,
                         *args, **kwargs)

    async def write(self, writer):
        """
        This method works strictly for ordered in depth folder tree records.
        See get_node_select_query and https://postgrespro.ru/docs/postgresql/15/queries-with#QUERIES-WITH-RECURSIVE
        """
        parents = deque()
        is_first_child = True
        async for row in self._value.records:
            # close previous parents if next node is outer
            while parents and row['parentId'] != parents[-1]:
                parents.pop()
                await writer.write(b']}')
                is_first_child = False

            # no need comma before first child in children list
            if not is_first_child:
                await writer.write(b',')
            else:
                is_first_child = False

            # todo: better (more efficient?) way to do it (without slice probably)?
            # write node record without "}"
            await writer.write(dumps(row).encode(self._encoding)[:-1])

            # write children field
            await writer.write(b', "children": ')
            if row['type'] == ItemType.FOLDER.value:
                parents.append(row['id'])
                is_first_child = True
                await writer.write(b'[')
            else:
                await writer.write(b'null}')

        # close remaining parents
        for _ in range(len(parents)):
            await writer.write(b']}')


class AsyncGenJsonListPayload(Payload):

    def __init__(self, value, encoding: str = 'utf-8',
                 content_type: str = 'application/json',
                 root_object: str = 'items',
                 *args, **kwargs):
        self.root_object = root_object
        super().__init__(value, content_type=content_type, encoding=encoding,
                         *args, **kwargs)

    async def write(self, writer):
        await writer.write(
            ('{"%s":[' % self.root_object).encode(self._encoding)
        )

        try:
            first_row = await anext(self._value)
            await writer.write(dumps(first_row).encode(self._encoding))
        except StopAsyncIteration:
            pass
        else:
            async for row in self._value:
                await writer.write(b',')
                await writer.write(dumps(row).encode(self._encoding))

        await writer.write(b']}')


__all__ = (
    'JsonPayload', 'AsyncGenJsonListPayload', 'JsonNodeTreeAsyncGenPayload'
)
