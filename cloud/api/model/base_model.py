from datetime import datetime

from aiohttp.web_exceptions import HTTPBadRequest
from asyncpgsa.connection import SAConnection

from .query_builder import insert_import_query


class NotInitializedError(Exception):
    pass


class BaseImportModel:

    __slots__ = ('_date', '_import_id', '_conn')

    def __init__(self, date: datetime | None):
        # todo: date is None in some cases. need check and refactor probably
        if date is not None and date.tzinfo is None:
            raise HTTPBadRequest

        self._date = date

        self._conn = None
        self._import_id = None

    async def init(self, connection):
        self._conn = connection

    @property
    def conn(self) -> SAConnection:
        if self._conn is None:
            raise NotInitializedError(f'{self.__class__.__name__} init() needs to be called first.')
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
        self._import_id = await self.conn.fetchval(insert_import_query(self.date))
