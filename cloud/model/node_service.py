from datetime import datetime

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from .import_model import ImportModel
from .node_model import NodeModel
from .base_service import BaseImportService, BaseService
from .schemas import ListResponseItem
from .node_tree import ResponseNodeTree
from cloud.utils import advisory_lock, QueueWorker
from ..db.schema import ItemType


class BaseNodeService(BaseService):
    __slots__ = ('_mdl', 'node_id')

    def __init__(self, pg, node_id: str, *args):
        super().__init__(pg, *args)
        self.node_id = node_id
        self._mdl = NodeModel(pg, node_id)

    @property
    def mdl(self) -> NodeModel:
        return self._mdl


class NodeService(BaseNodeService):

    def __init__(self, pg, node_id: str):
        super().__init__(pg, node_id)

    async def get_node(self) -> ResponseNodeTree:
        await self.mdl.init()
        # In general from_records returns a list[NodeTree]. In this case it will always be a single NodeTree list.
        tree = ResponseNodeTree.from_records(self.mdl.get_node())[0]
        return tree

    async def get_node_history(self, date_start: datetime, date_end: datetime) -> ListResponseItem:
        await self.mdl.init()
        res = await self.mdl.get_node_history(date_start, date_end)
        items = ListResponseItem(items=[{'type': self.mdl.node_type, **rec} for rec in res])
        return items


class NodeImportService(BaseNodeService, BaseImportService):

    def __init__(self, pg: PG, node_id: str, date: datetime):
        super().__init__(pg, node_id, date)

    @property
    def _import_mdl_id_params(self):
        if self.mdl.node_type == ItemType.FILE:
            return self.node_id, None
        else:
            return None, self.node_id

    async def create_models(self, conn: SAConnection):
        self._mdl = NodeModel(conn, self.node_id)

    async def init_models(self):
        await self.mdl.init()

        self._import_mdl = ImportModel(conn, *self._import_mdl_id_params)

        parents = self.query.recursive_parents(self.node_id).select()
        history_q = FolderQuery.insert_history_from_select(parents)

        update_q = import_queries.update_parent_sizes(
            file_id, folder_id, import_id,
            import_queries.Sign.SUB
        )

        await self.conn.execute(history_q)
        await self.conn.execute(update_q)

        self._import_mdl = ImportModel(conn, )

    async def execute_delete_node(self):
        async with QueueWorker(self.date) as qw:
            import_id = qw.queue_id
            async with self.pg.transaction() as conn:
                await self.init_models(conn)

                await self.import_mdl.lock_branches()

                await self.import_mdl.insert_import(import_id, self.date)
                await self.import_mdl.write_folders_history()
                await self.import_mdl.subtract_parent_sizes(*self._import_mdl_id_params)
                await self.mdl.delete_node(self.import_id)
