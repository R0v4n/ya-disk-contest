from datetime import datetime, timedelta

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from disk.models import ItemType, ListResponseItem
from disk.db.repositories import HistoryRepository
from disk.services.base import BaseService


class HistoryService(BaseService):
    __slots__ = ('date', '_repo')

    def __init__(self, pg: PG, date: datetime):
        super().__init__(pg)
        self.date = date
        self._repo = None

    @property
    def repo(self) -> HistoryRepository:
        return self._repo

    async def init_repos(self, conn: SAConnection | PG):
        self._repo = HistoryRepository(conn)

    async def get_files_updates(self, days: int = 1) -> ListResponseItem:
        await self.init_repos(self.pg)
        date_start = self.date - timedelta(days=days)
        records = await self.repo.get_files_updates_daterange(date_start, self.date)

        items = ListResponseItem(items=[{'type': ItemType.FILE, **rec} for rec in records])
        return items
