from datetime import datetime, timedelta

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from cloud.model import HistoryModel, ItemType, ListResponseItem
from cloud.services.base import BaseService


class HistoryService(BaseService):
    __slots__ = ('date', '_mdl')

    def __init__(self, pg: PG, date: datetime):
        super().__init__(pg)
        self.date = date
        self._mdl = None

    @property
    def mdl(self) -> HistoryModel:
        return self._mdl

    async def init_models(self, conn: SAConnection | PG):
        self._mdl = HistoryModel(conn)

    async def get_files_updates(self, days: int = 1) -> ListResponseItem:
        await self.init_models(self.pg)
        date_start = self.date - timedelta(days=days)
        records = await self.mdl.get_files_updates_daterange(date_start, self.date)

        items = ListResponseItem(items=[{'type': ItemType.FILE, **rec} for rec in records])
        return items
