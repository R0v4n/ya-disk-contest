from datetime import datetime

from .base import BaseImportModel, BaseModel
from .exceptions import ModelValidationError, ItemNotFoundError
from .schemas import ItemType, ListResponseItem
from .node_tree import ResponseNodeTree
from cloud.queries import FileQuery, FolderQuery, QueryT, import_queries
from cloud.utils import advisory_lock, QueueWorker


class NodeBaseModel(BaseModel):

    def __init__(self, node_id: str, *args):
        super().__init__(*args)
        self.node_id = node_id
        self._query = None
        self._node_type = None

    @property
    def query(self) -> type[QueryT]:
        return self._query

    @property
    def node_type(self) -> ItemType:
        return self._node_type

    async def init(self, connection):
        await super().init(connection)
        await self.define_node_type()

    async def define_node_type(self):
        for self._query, self._node_type in zip([FileQuery, FolderQuery], ItemType):
            node_exists = await self.conn.fetchval(self._query.exist(self.node_id))

            if node_exists:
                return

        raise ItemNotFoundError


class NodeModel(NodeBaseModel):

    __slots__ = ('node_id', '_query', '_node_type')

    def __init__(self, node_id: str):
        super().__init__(node_id)

    async def get_node(self) -> ResponseNodeTree:
        res = await self.conn.fetch(self.query.get_node_select_query(self.node_id))
        # In general from_records returns a list[NodeTree]. In this case it will always be a single NodeTree list.
        tree = ResponseNodeTree.from_records(res)[0]
        return tree

    async def get_node_history(self, date_start: datetime, date_end: datetime) -> ListResponseItem:
        if date_start >= date_end or date_end.tzinfo is None or date_start.tzinfo is None:
            raise ModelValidationError

        query = self.query.select_nodes_union_history_in_daterange(
            date_start, date_end, self.node_id, closed=False)

        res = await self.conn.fetch(query)
        items = ListResponseItem(items=[{'type': self.node_type, **rec} for rec in res])
        return items


class NodeImportModel(BaseImportModel, NodeBaseModel):

    __slots__ = ('node_id', '_query', '_node_type')

    def __init__(self, node_id: str, date: datetime):
        super().__init__(date, node_id)

    async def _delete_node(self):
        async with advisory_lock(self.conn, 0):
            QueueWorker.release_queue(self.import_id)

            await self.acquire_advisory_xact_lock_by_ids([self.node_id])
            await self.define_node_type()
            await self.conn.execute(self.query.xact_advisory_lock_parent_ids([self.node_id]))

        await self.insert_import()
        parents = self.query.recursive_parents(self.node_id).select()
        history_q = FolderQuery.insert_history_from_select(parents)

        file_id, folder_id = (self.node_id, None) if self.node_type == ItemType.FILE else (None, self.node_id)
        update_q = import_queries.update_parent_sizes(
            file_id, folder_id, self.import_id,
            import_queries.Sign.SUB
        )

        await self.conn.execute(history_q)
        await self.conn.execute(update_q)
        await self.conn.execute(self.query.delete(self.node_id))

    async def execute_delete_node(self):
        await self._execute_in_import_transaction(self._delete_node())
