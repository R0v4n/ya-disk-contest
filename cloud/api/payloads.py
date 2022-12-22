import json
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
    """
    Заменяет функцию сериализации на более "умную" (умеющую упаковывать в JSON
    объекты asyncpg.Record и другие сущности).
    """
    def __init__(self,
                 value: Any,
                 encoding: str = 'utf-8',
                 content_type: str = 'application/json',
                 dumps: JSONEncoder = dumps,
                 *args: Any,
                 **kwargs: Any) -> None:
        super().__init__(value, encoding, content_type, dumps, *args, **kwargs)


class AsyncGenJsonListPayload(Payload):
    """
    Итерируется по объектам AsyncIterable, частями сериализует данные из них
    в JSON и отправляет клиенту.
    """
    def __init__(self, value, encoding: str = 'utf-8',
                 content_type: str = 'application/json',
                 root_object: str = 'items',
                 *args, **kwargs):
        self.root_object = root_object
        super().__init__(value, content_type=content_type, encoding=encoding,
                         *args, **kwargs)

    async def write(self, writer):
        # Начало объекта
        await writer.write(
            ('{"%s":[' % self.root_object).encode(self._encoding)
        )

        first = True
        async for row in self._value:
            # Перед первой строчкой запятая не нужнаа
            if not first:
                await writer.write(b',')
            else:
                first = False

            await writer.write(dumps(row).encode(self._encoding))

        # Конец объекта
        await writer.write(b']}')


__all__ = (
    'JsonPayload', 'AsyncGenJsonListPayload'
)
