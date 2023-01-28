from datetime import datetime, timedelta

from .base import BaseModel
from .schemas import ItemType, ListResponseItem
from cloud.queries import FileQuery


class HistoryModel(BaseModel):
    __slots__ = ('date_end',)

    def __init__(self, date: datetime):
        super().__init__()
        self.date_end = date

    async def get_files_updates(self, days: int = 1) -> ListResponseItem:
        date_start = self.date_end - timedelta(days=days)

        query = FileQuery.select_updates_daterange(date_start, self.date_end)

        res = await self.conn.fetch(query)

        items = ListResponseItem(items=[{'type': ItemType.FILE, **rec} for rec in res])
        return items
