from datetime import datetime

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from disk.models import ResponseNodeTree, ListResponseItem, ItemType
from .base import BaseImportService, BaseNodeService


class NodeService(BaseNodeService):
    __slots__ = ('_repo', 'node_id')

    def __init__(self, pg: PG, node_id: str):
        super().__init__(pg, node_id)

    async def get_node(self) -> ResponseNodeTree:
        async with self.pg.pool.acquire() as conn:
            await self.init_repos(conn)
            records = await self.repo.get_node()

        # In general from_records returns a list[NodeTree]. In this case it will always be a single NodeTree list.
        tree = ResponseNodeTree.from_records(records)[0]
        return tree

    async def get_node_history(self, date_start: datetime, date_end: datetime) -> ListResponseItem:
        await self.init_repos(self.pg)
        res = await self.repo.get_node_history(date_start, date_end)

        items = ListResponseItem(items=[{'type': self.repo.node_type, **rec} for rec in res])
        return items


class NodeImportService(BaseImportService, BaseNodeService):
    __slots__ = ('_repo', 'node_id')

    def __init__(self, pg: PG, node_id: str, date: datetime):
        super().__init__(pg, date, node_id)

    @property
    def _import_repo_id_params(self):
        if self.repo.node_type == ItemType.FOLDER:
            return self.node_id, None
        else:
            return None, self.node_id

    async def init_repos(self, conn: SAConnection):
        await self.import_repo.lock_ids((self.node_id,))
        await super().init_repos(conn)
        await self.import_repo.lock_branches(*self._import_repo_id_params)
        self.import_repo.release_queue()

    async def _delete_node(self):
        await self.repo.write_parents_to_history()
        await self.import_repo.subtract_parent_sizes(*self._import_repo_id_params)
        await self.repo.delete_node()

    async def execute_delete_node(self):
        await self._execute_in_import_transaction(self._delete_node())
