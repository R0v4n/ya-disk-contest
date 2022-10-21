from datetime import datetime

from asyncpgsa.connection import SAConnection

from .query_builder import ImportQuery


class BaseModel:
    def __init__(self, conn: SAConnection, date: datetime):
        self._conn = conn
        self._date = date

        self._import_id = None

    @property
    def conn(self):
        return self._conn

    @property
    def date(self):
        return self._date

    @property
    def import_id(self) -> int:
        if self._import_id is None:
            raise AttributeError(f'Need to call "self.insert_import" first to init import_id reference.')
        return self._import_id

    async def insert_import(self):
        query = ImportQuery.insert_import(self.date)

        self._import_id = await self.conn.fetchval(query)
