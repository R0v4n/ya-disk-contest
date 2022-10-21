from datetime import datetime, timedelta
from typing import Any

from asyncpg import Record
from asyncpgsa import PG
from pydantic import ValidationError

from cloud.api.model.query_builder import FileQuery, FolderQuery
from cloud.api.model.data_classes import ExportNode, NodeType
from cloud.utils.pg import DEFAULT_PG_URL


class HistoryModel:

    def __init__(self, pg: PG, date_end: datetime):

        self.conn = pg
        self.date_end = date_end

    async def get_files_updates_24h(self) -> list[dict[str, Any]]:
        date_start = self.date_end - timedelta(1)

        query = FileQuery.select_updates_daterange(date_start, self.date_end)

        res = await self.conn.fetch(query)
        nodes = [ExportNode(type=NodeType.FILE, **rec).dict(by_alias=True) for rec in res]
        return nodes


if __name__ == '__main__':
    import asyncio
    from devtools import debug


    async def get_node():
        pg = PG()
        await pg.init(DEFAULT_PG_URL)

        de = datetime.fromisoformat('2022-02-03 12:00:00 +00:00')

        mdl = HistoryModel(pg, date_end=de)
        try:
            nodes = await mdl.get_files_updates_24h()
        except ValidationError as err:
            debug(err)
        debug(nodes)


    async def main():
        await get_node()


    asyncio.run(main())