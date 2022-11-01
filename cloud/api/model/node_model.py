from datetime import datetime
from typing import Any

from aiohttp.web_exceptions import HTTPNotFound
from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from cloud.api.model.base_model import BaseModel
from cloud.api.model.data_classes import NodeType, ExportNode
from cloud.api.model.node_tree import ExportNodeTree
from cloud.api.model.query_builder import FileQuery, FolderQuery, NodeQuery, ImportQuery
from cloud.utils.pg import DEFAULT_PG_URL


class NodeModel(BaseModel):
    def __init__(self, node_id: str, conn: SAConnection, date: datetime | None = None):

        super().__init__(conn, date)
        self.node_id = node_id

    async def get_node_type(self) -> NodeType:
        for q, t in zip([FileQuery, FolderQuery], NodeType):
            node_exists = await self.conn.fetchval(q.exist(self.node_id))

            if node_exists:
                return t

        raise HTTPNotFound()

    async def get_node(self) -> dict[str, Any]:
        if await self.get_node_type() == NodeType.FILE:
            query = FileQuery.select_node_with_date(self.node_id, ['id', 'parent_id', 'url', 'size'])
            res = [await self.conn.fetchrow(query)]

        else:
            queries = NodeQuery(self.node_id)

            res = await self.conn.fetch(queries.folder_children())
            res += await self.conn.fetch(queries.file_children())

        # In general from_records returns a list[NodeTree]. In this case it will always be a single NodeTree list.
        tree = ExportNodeTree.from_records(res)[0]

        return tree.dict(by_alias=True)

    async def delete_node(self):
        query_class, file_id, folder_id = {
            NodeType.FILE: (FileQuery, self.node_id, []),
            NodeType.FOLDER: (FolderQuery, [], self.node_id)
        }[await self.get_node_type()]

        await self.insert_import()

        # todo: this can be done with ImportQuery. Need Query classes refactor
        parents = query_class.recursive_parents(self.node_id)
        history_q = FolderQuery.insert_history_from_select(parents)

        update_q = ImportQuery(file_id, folder_id, self.import_id).update_folder_sizes(add=False)

        await self.conn.execute(history_q)
        await self.conn.execute(update_q)
        # await self.conn.execute(query_class.subtract_parents_size(self.node_id))
        await self.conn.execute(query_class.delete(self.node_id))

    async def get_node_history(self, date_start: datetime, date_end: datetime) -> list[dict[str, Any]]:
        # todo: dry!
        node_type = await self.get_node_type()
        query_class = {
            NodeType.FILE: FileQuery,
            NodeType.FOLDER: FolderQuery
        }[node_type]

        query = query_class.select_nodes_union_history_in_daterange(date_start, date_end, self.node_id, closed=False)

        res = await self.conn.fetch(query)
        nodes = [ExportNode(type=node_type, **rec).dict(by_alias=True) for rec in res]

        return nodes


if __name__ == '__main__':
    import asyncio
    from devtools import debug


    async def get_node():
        pg = PG()
        await pg.init(DEFAULT_PG_URL)

        async with pg.transaction() as conn:
            mdl = NodeModel('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1', conn)
            node = await mdl.get_node()
            debug(node)

    async def get_file():
        pg = PG()
        await pg.init(DEFAULT_PG_URL)

        async with pg.transaction() as conn:
            mdl = NodeModel('863e1a7a-1304-42ae-943b-179184c077e3', conn)
            node = await mdl.get_node()
            debug(node)

    async def del_node():
        pg = PG()
        await pg.init(DEFAULT_PG_URL)

        async with pg.transaction() as conn:
            mdl = NodeModel('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1', conn)
            await mdl.delete_node()

    async def node_history():
        pg = PG()
        await pg.init(DEFAULT_PG_URL)

        ds = datetime.fromisoformat('2021-10-03 12:00:00 +00:00')
        de = datetime.fromisoformat('2022-10-03 12:00:00 +00:00')

        async with pg.transaction() as conn:
            mdl = NodeModel('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1', conn)
            nodes = await mdl.get_node_history(ds, de)
            debug(nodes)

    async def main():
        await get_node()
        # await del_node()
        # await node_history()
        # await get_file()

    asyncio.run(main())
