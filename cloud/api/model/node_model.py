from datetime import datetime
from typing import Any

from aiohttp.web_exceptions import HTTPNotFound, HTTPBadRequest
from asyncpgsa import PG

from .base_model import BaseImportModel
from .data_classes import ItemType, ExportItem
from .node_tree import ExportNodeTree
from .query_builder import FileQuery, FolderQuery, NodeQuery, ImportQuery


class NodeModel(BaseImportModel):
    def __init__(self, node_id: str, pg: PG, date: datetime | None = None):

        super().__init__(pg, date)
        self.node_id = node_id

    async def _get_node_type(self, conn) -> ItemType:
        for q, t in zip([FileQuery, FolderQuery], ItemType):
            node_exists = await conn.fetchval(q.exist(self.node_id))

            if node_exists:
                return t

        raise HTTPNotFound()

    async def get_node(self) -> dict[str, Any]:
        if await self._get_node_type(self.pg) == ItemType.FILE:
            query = FileQuery.select_node_with_date(self.node_id, ['id', 'parent_id', 'url', 'size'])
            res = [await self.pg.fetchrow(query)]

        else:
            queries = NodeQuery(self.node_id)

            res = await self.pg.fetch(queries.folder_children())
            res += await self.pg.fetch(queries.file_children())

        # In general from_records returns a list[NodeTree]. In this case it will always be a single NodeTree list.
        tree = ExportNodeTree.from_records(res)[0]

        return tree.dict(by_alias=True)

    async def _delete_node(self):
        # todo: why arg node_id is str?
        query_class, file_id, folder_id = {
            ItemType.FILE: (FileQuery, self.node_id, []),
            ItemType.FOLDER: (FolderQuery, [], self.node_id)
        }[await self._get_node_type(self.conn)]

        await self.insert_import()

        parents = query_class.recursive_parents(self.node_id)
        history_q = FolderQuery.insert_history_from_select(parents)

        update_q = ImportQuery(file_id, folder_id, self.import_id).update_folder_sizes(add=False)

        await self.conn.execute(history_q)
        await self.conn.execute(update_q)
        # await self.conn.execute(query_class.subtract_parents_size(self.node_id))
        await self.conn.execute(query_class.delete(self.node_id))

    async def execute_del_node(self):
        await self.execute_in_transaction(self._delete_node)

    async def get_node_history(self, date_start: datetime, date_end: datetime) -> list[dict[str, Any]]:

        if date_start >= date_end or date_end.tzinfo is None or date_start.tzinfo is None:
            raise HTTPBadRequest

        # todo: dry!
        node_type = await self._get_node_type(self.pg)
        query_class = {
            ItemType.FILE: FileQuery,
            ItemType.FOLDER: FolderQuery
        }[node_type]

        query = query_class.select_nodes_union_history_in_daterange(date_start, date_end, self.node_id, closed=False)

        res = await self.pg.fetch(query)
        nodes = [ExportItem(type=node_type, **rec).dict(by_alias=True) for rec in res]

        return nodes
