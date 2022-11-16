from datetime import datetime, timedelta
from typing import Any

from asyncpgsa import PG

from .data_classes import ExportItem, ItemType
from .query_builder import FileQuery


class HistoryModel:

    def __init__(self, pg: PG, date_end: datetime):

        self.conn = pg
        self.date_end = date_end

    async def get_files_updates_24h(self) -> list[dict[str, Any]]:
        date_start = self.date_end - timedelta(days=1)

        query = FileQuery.select_updates_daterange(date_start, self.date_end)

        res = await self.conn.fetch(query)

        # todo: do i really need using ExportItem here? performance?
        #  json payloads?
        nodes = [ExportItem(type=ItemType.FILE, **rec).dict(by_alias=True) for rec in res]
        return nodes
