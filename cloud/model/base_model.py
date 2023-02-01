from abc import ABC, abstractmethod
from datetime import datetime

from asyncpgsa.connection import SAConnection

from cloud.queries import import_queries


class BaseModel(ABC):
    __slots__ = '_conn',

    def __init__(self, conn: SAConnection):
        self._conn = conn

    @property
    def conn(self) -> SAConnection:
        return self._conn

    @abstractmethod
    async def init(self):
        """some initialization db queries"""


class BaseImportModel(BaseModel, ABC):
    __slots__ = '_import_id',

    def __init__(self, conn: SAConnection):
        super().__init__(conn)
        self._import_id = None

    @property
    def import_id(self):
        return self._import_id

    @import_id.setter
    def import_id(self, value: int):
        self._import_id = value

    # note: not used
    async def insert_import_auto_id(self, date: datetime):
        self._import_id = await self.conn.fetchval(
            import_queries.insert_import_auto_id(date))

    async def insert_import(self, import_id: int, date: datetime):
        self._import_id = import_id
        await self.conn.execute(
            import_queries.insert_import(self._import_id, date))


