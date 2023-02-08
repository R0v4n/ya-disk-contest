from datetime import datetime

from asyncpg import Record

from .base import BaseRepository
from cloud.db.queries import FileQuery


class HistoryRepository(BaseRepository):
    async def get_files_updates_daterange(
            self,
            date_start: datetime,
            date_end: datetime
    ) -> list[Record]:

        query = FileQuery.select_updates_daterange(date_start, date_end)
        return await self.conn.fetch(query)

