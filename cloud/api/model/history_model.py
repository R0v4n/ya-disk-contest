from datetime import datetime, timedelta
from typing import Any, AsyncGenerator

from asyncpgsa import PG

from .data_classes import ExportItem, ItemType
from .query_builder import FileQuery

from cloud.utils.pg import select_query_async_gen


class HistoryModel:

    __slots__ = ('conn', 'date_end')

    def __init__(self, pg: PG, date_end: datetime):

        self.conn = pg
        self.date_end = date_end

    async def get_files_updates_24h(self) -> AsyncGenerator[dict[str, Any], Any]:
        date_start = self.date_end - timedelta(days=1)

        query = FileQuery.select_updates_daterange(date_start, self.date_end)

        return select_query_async_gen(
            query,
            self.conn.transaction(),
            lambda rec: ExportItem.construct(type=ItemType.FILE, **rec).dict(by_alias=True)
        )
