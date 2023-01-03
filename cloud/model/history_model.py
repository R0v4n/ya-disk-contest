from datetime import datetime, timedelta
from typing import Any

from asyncpgsa import PG

from .data_classes import ExportItem, ItemType, ListExportItems
from .query_builder import FileQuery


class HistoryModel:
    __slots__ = ('conn', 'date_end')

    def __init__(self, pg: PG, date_end: datetime):
        self.conn = pg
        self.date_end = date_end

    async def get_files_updates_24h(self) -> ListExportItems:
        date_start = self.date_end - timedelta(days=1)

        query = FileQuery.select_updates_daterange(date_start, self.date_end)

        res = await self.conn.fetch(query)

        items = ListExportItems(items=[{'type': ItemType.FILE, **rec} for rec in res])
        return items
