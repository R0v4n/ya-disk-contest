import logging
import asyncio
from datetime import datetime
from typing import Iterable

from asyncpgsa.connection import SAConnection

from .exceptions import NotInitializedError, ModelValidationError
from .query_builder import (
    insert_import_query, insert_queue_query, insert_import_from_mdl_query
)
from cloud.api_fastapi.events import tasks_set, events

logger = logging.getLogger(__name__)


class BaseModel:

    __slots__ = ('_conn',)

    def __init__(self, *args):
        self._conn = None

    async def init(self, connection):
        self._conn = connection

    @property
    def conn(self) -> SAConnection:
        if self._conn is None:
            raise NotInitializedError(f'{self.__class__.__name__}.init needs to be called first.')
        return self._conn

    async def acquire_advisory_xact_lock_by_ids(self, ids: Iterable[str]):
        if ids:
            query = 'VALUES '
            query += ', '.join(f"(pg_advisory_xact_lock(hashtextextended('{i}', 0)))" for i in ids)
            await self.conn.execute(query)

    async def acquire_advisory_lock(self, i: int):
        await self.conn.execute('SELECT pg_advisory_lock($1)', i)

    async def release_advisory_lock(self, i: int):
        await self.conn.execute('SELECT pg_advisory_unlock($1)', i)


class BaseImportModel(BaseModel):

    __slots__ = ('_date', '_import_id', '_queue_id')

    def __init__(self, date: datetime, *args):
        super().__init__(*args)

        self.date = date
        self._import_id = None
        self._queue_id = None

    @property
    def date(self) -> datetime:
        return self._date

    @date.setter
    def date(self, date: datetime):
        if date.tzinfo is None:
            raise ModelValidationError
        self._date = date

    @property
    def import_id(self) -> int:
        if self._import_id is None:
            raise NotInitializedError(f'{self.__class__.__name__}.insert_import needs to be called first.')
        return self._import_id

    @property
    def queue_id(self) -> int:
        if self._queue_id is None:
            raise NotInitializedError(f'{self.__class__.__name__}.insert_queue needs to be called first.')
        return self._queue_id

    # note: not used
    async def insert_import(self):
        self._import_id = await self.conn.fetchval(insert_import_query(self.date))

    async def insert_import_with_model_id(self):
        await self.conn.execute(insert_import_from_mdl_query(self._import_id, self._date))

    async def insert_queue(self):
        self._queue_id = await self.conn.fetchval(insert_queue_query(self.date))

    # note: this method is just my adhoc experiment to handle simultaneous imports requests.
    async def wait_queue(self):
        await self.insert_queue()
        await asyncio.sleep(0.02)

        self._import_id = self._queue_id
        event = asyncio.Event()
        events[self._queue_id] = event
        waiter_task = asyncio.create_task(event.wait())
        waiter_task.add_done_callback(tasks_set.discard)
        tasks_set.add(waiter_task)

        await waiter_task


