from abc import abstractmethod
from datetime import datetime
from typing import Callable, Coroutine

from aiohttp.web_exceptions import HTTPBadRequest
from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from .query_builder import ImportQuery


class BaseImportModel:
    def __init__(self, pg: PG, date: datetime | None):
        # todo: date is None in some cases. need check and refactor probably
        if date is not None and date.tzinfo is None:
            raise HTTPBadRequest

        self.pg = pg
        self._date = date

        self._conn = None
        self._import_id = None

    async def execute_in_transaction(self, *methods):
        async with self.pg.transaction() as conn:
            self._conn = conn
            for m in methods:
                await m()

        self._conn = None

    @property
    def conn(self) -> SAConnection:
        if self._conn is None:
            raise AttributeError(f'Any method using connection should be passed to self.execute_in_transaction.')
        return self._conn

    @property
    def date(self) -> datetime:
        return self._date

    @property
    def import_id(self) -> int:
        if self._import_id is None:
            raise AttributeError(f'Need to call "self.insert_import" first to init import_id reference.')
        return self._import_id

    async def insert_import(self):
        query = ImportQuery.insert_import(self.date)

        self._import_id = await self.conn.fetchval(query)
