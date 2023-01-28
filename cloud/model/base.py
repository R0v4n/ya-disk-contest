import logging
from datetime import datetime
from typing import Iterable, Coroutine

from asyncpgsa.connection import SAConnection

from .exceptions import NotInitializedError, ModelValidationError
from cloud.queries import import_queries
from cloud.utils import QueueWorker

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


class BaseImportModel(BaseModel):

    __slots__ = ('_date', '_import_id')

    def __init__(self, date: datetime, *args):
        super().__init__(*args)

        self.date = date
        self._import_id = None

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

    @import_id.setter
    def import_id(self, value: int):
        self._import_id = value

    # note: not used
    async def insert_import_auto_id(self):
        self._import_id = await self.conn.fetchval(
            import_queries.insert_import_auto_id(self.date))

    async def insert_import(self):
        await self.conn.execute(
            import_queries.insert_import(self._import_id, self._date))

    async def _execute_in_import_transaction(self, coro: Coroutine):
        async with QueueWorker(self._date) as qw:
            self._import_id = qw.queue_id

            async with self.conn.transaction() as conn:
                self._conn = conn
                await coro
                self._conn = None
