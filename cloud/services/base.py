import logging
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Coroutine

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from cloud.model import NodeModel, ImportModel
from cloud.utils import QueueWorker
from cloud.model.exceptions import ModelValidationError


logger = logging.getLogger(__name__)


class BaseService(ABC):

    __slots__ = ('_pg',)

    def __init__(self, pg: PG, *args):
        self._pg = pg

    @property
    def pg(self) -> PG:
        return self._pg

    @abstractmethod
    async def init_models(self, conn: SAConnection | PG):
        """create and init models"""


class BaseImportService(BaseService):

    __slots__ = ('_date', '_import_mdl')

    def __init__(self, pg: PG, date: datetime, *args):
        super().__init__(pg, *args)

        self.date = date
        self._import_mdl = None

    @property
    def date(self) -> datetime:
        return self._date

    @date.setter
    def date(self, date: datetime):
        if date.tzinfo is None:
            raise ModelValidationError
        self._date = date

    @property
    def import_mdl(self) -> ImportModel:
        return self._import_mdl

    @abstractmethod
    async def init_models(self, conn: SAConnection):
        await super().init_models(conn)

    async def _execute_in_import_transaction(self, coro: Coroutine):
        async with QueueWorker(self._date) as qw:
            async with self.pg.transaction() as conn:
                self._import_mdl = ImportModel(conn, qw.queue_id)
                await self.init_models(conn)
                await self.import_mdl.insert_import(self.date)

                await coro


class BaseNodeService(BaseService):

    def __init__(self, pg: PG, node_id: str, *args):
        super().__init__(pg, *args)
        self.node_id = node_id
        self._mdl = None

    @property
    def mdl(self) -> NodeModel:
        return self._mdl

    async def init_models(self, conn: SAConnection | PG):
        await super().init_models(conn)
        self._mdl = NodeModel(conn, self.node_id)
        await self.mdl.init()
