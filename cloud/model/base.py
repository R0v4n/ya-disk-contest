import asyncio
from datetime import datetime

from asyncpgsa.connection import SAConnection

from .exceptions import NotInitializedError, ModelValidationError
from .query_builder import (
    insert_import_query, insert_queue_query, insert_import_from_mdl_query,
    get_oldest_queue_id, delete_queue
)


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

    async def acquire_imports_table_lock(self):
        await self.conn.execute('LOCK TABLE imports IN SHARE ROW EXCLUSIVE MODE;')

    # note: not used
    async def insert_import(self):
        await self.acquire_imports_table_lock()
        self._import_id = await self.conn.fetchval(insert_import_query(self.date))

    # note: lock imports table!
    async def insert_import_with_model_id(self):
        await self.acquire_imports_table_lock()
        await self.conn.execute(insert_import_from_mdl_query(self._import_id, self._date))

    async def insert_queue(self):
        self._queue_id = await self.conn.fetchval(insert_queue_query(self.date))

    # note: this method is just my adhoc experiment to handle simultaneous imports requests.
    #  Another way is to create full import data queue table. But it is a lot of work to do
    #  (need refactor ImportModel and queries) and it doesn't make sense for me to do it right now.
    async def queue_wait(self):
        await self.insert_queue()
        while True:
            await asyncio.sleep(0.05)
            oldest_queue_id = await self.conn.fetchval(get_oldest_queue_id())
            if oldest_queue_id == self._queue_id:
                self._import_id = oldest_queue_id
                await self.conn.execute(delete_queue(oldest_queue_id))
                return
