import logging
from abc import abstractmethod
from datetime import datetime
from typing import Coroutine

from asyncpgsa.connection import SAConnection

from cloud.utils import QueueWorker
from .exceptions import NotInitializedError, ModelValidationError
from .import_model import ImportModel

logger = logging.getLogger(__name__)


class BaseService:

    __slots__ = ('_pg',)

    def __init__(self, pg, *args):
        self._pg = pg

    @property
    def pg(self) -> SAConnection:
        if self._pg is None:
            raise NotInitializedError(f'{self.__class__.__name__}.init needs to be called first.')
        return self._pg


class BaseImportService(BaseService):

    __slots__ = ('_date', '_import_mdl')

    def __init__(self, pg, date: datetime, *args):
        super().__init__(pg)

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
    def create_models(self, conn: SAConnection):
        pass

    @abstractmethod
    async def init_models(self):
        await self.import_mdl.lock_branches()

    async def _execute_in_import_transaction(self, coro: Coroutine):
        async with QueueWorker(self._date) as qw:
            async with self.pg.transaction() as conn:
                self.create_models(conn)
                await self.init_models()
                await self.import_mdl.insert_import(qw.queue_id, self.date)

                await coro
