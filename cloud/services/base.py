import logging
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Coroutine

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from cloud.db.repositories import NodeRepository, ImportRepository
from cloud.utils import QueueWorker


logger = logging.getLogger(__name__)


class BaseService(ABC):

    __slots__ = ('_pg',)

    def __init__(self, pg: PG, *args):
        self._pg = pg

    @property
    def pg(self) -> PG:
        return self._pg

    @abstractmethod
    async def init_repos(self, conn: SAConnection | PG):
        """create and init repositories"""


class BaseImportService(BaseService):

    __slots__ = ('_date', '_import_repo')

    def __init__(self, pg: PG, date: datetime, *args):
        super().__init__(pg, *args)

        self.date = date
        self._import_repo = None

    @property
    def date(self) -> datetime:
        return self._date

    @date.setter
    def date(self, date: datetime):
        self._date = date

    @property
    def import_repo(self) -> ImportRepository:
        return self._import_repo

    @abstractmethod
    async def init_repos(self, conn: SAConnection):
        await super().init_repos(conn)

    async def _execute_in_import_transaction(self, coro: Coroutine):
        async with QueueWorker(self._date) as qw:
            async with self.pg.transaction() as conn:
                self._import_repo = ImportRepository(conn, qw.queue_id)
                await self.init_repos(conn)
                await self.import_repo.insert_import(self.date)

                await coro


class BaseNodeService(BaseService):

    def __init__(self, pg: PG, node_id: str, *args):
        super().__init__(pg, *args)
        self.node_id = node_id
        self._repo = None

    @property
    def repo(self) -> NodeRepository:
        return self._repo

    async def init_repos(self, conn: SAConnection | PG):
        await super().init_repos(conn)
        self._repo = NodeRepository(conn, self.node_id)
        await self.repo.init()
