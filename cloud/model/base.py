from datetime import datetime

from asyncpgsa.connection import SAConnection

from .exceptions import NotInitializedError, ModelValidationError
from .query_builder import insert_import_query


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

    async def insert_import(self):
        self._import_id = await self.conn.fetchval(insert_import_query(self.date))
