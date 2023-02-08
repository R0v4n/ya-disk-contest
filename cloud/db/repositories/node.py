from datetime import datetime

from asyncpg import Record
from asyncpgsa.connection import SAConnection

from cloud.db.schema import ItemType
from cloud.db.queries import QueryT, FileQuery, FolderQuery
from .base import BaseInitRepository
from .exceptions import ItemNotFoundError, ModelValidationError


class NodeRepository(BaseInitRepository):

    def __init__(self, conn: SAConnection, node_id: str):
        super().__init__(conn)
        self.node_id = node_id

        self._query = None
        self._node_type = None

    @property
    def query(self) -> type[QueryT]:
        return self._query

    @property
    def node_type(self) -> ItemType:
        return self._node_type

    async def init(self):
        for self._query, self._node_type in zip([FileQuery, FolderQuery], ItemType):
            node_exists = await self.conn.fetchval(self._query.exist(self.node_id))
            if node_exists:
                return

        raise ItemNotFoundError

    async def get_node(self) -> list[Record]:
        res = await self.conn.fetch(self.query.get_node_select_query(self.node_id))
        return res

    async def get_node_history(self, date_start: datetime, date_end: datetime) -> list[Record]:
        if date_start >= date_end or date_end.tzinfo is None or date_start.tzinfo is None:
            raise ModelValidationError

        query = self.query.select_nodes_union_history_in_daterange(
            date_start, date_end, self.node_id, closed=False)

        return await self.conn.fetch(query)

    async def delete_node(self):
        await self.conn.execute(self.query.delete(self.node_id))

    async def write_parents_to_history(self):
        parents = self.query.recursive_parents(self.node_id).select()
        history_q = FolderQuery.insert_history_from_select(parents)
        await self.conn.execute(history_q)
