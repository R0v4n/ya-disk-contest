from abc import abstractmethod
from typing import Iterable, Any

from asyncpg import ForeignKeyViolationError
from asyncpgsa.connection import SAConnection

from disk.db.queries import QueryT, FileQuery, FolderQuery
from .base import BaseInitRepository
from .exceptions import ParentNotFoundError, ModelValidationError
from disk.models import RequestNodeTree, ItemType, RequestItem


class ItemListBaseRepository(BaseInitRepository):
    NodeT: ItemType
    Query: type[QueryT]
    field_mapping: dict[str, str]

    __slots__ = ('nodes', 'ids', '_new_ids', '_existent_ids')

    def __init__(self, conn: SAConnection, nodes: dict[str, RequestItem]):

        super().__init__(conn)

        self.nodes = nodes
        self.ids = set(self.nodes.keys())

        self._new_ids = None
        self._existent_ids = None

    @property
    def new_ids(self) -> set[str]:
        return self._new_ids

    @property
    def existent_ids(self) -> set[str]:
        return self._existent_ids

    async def init(self):
        self._existent_ids = await self._get_existent_ids()
        self._new_ids = self.ids - self.existent_ids

    async def _get_existent_ids(self) -> set[str]:
        if self.ids:
            query = self.Query.select(self.ids, ['id'])
            # noinspection PyTypeChecker
            return {x['id'] for x in await self.conn.fetch(query)}
        else:
            return set()

    async def any_id_exists(self, ids: str | Iterable[str]) -> bool:
        return await self.conn.fetchval(self.Query.exist(ids))

    async def check_ids_not_exist(self, ids: str | Iterable[str]) -> None:
        exist = await self.any_id_exists(ids)
        if exist:
            raise ModelValidationError(f'Some ids already exists in {self.__class__.__name__}')

    async def insert_new(self, import_id: int):
        if self.new_ids:
            try:
                await self.conn.execute(self.Query.insert(self._get_new_nodes_records(import_id)))
            except ForeignKeyViolationError as err:
                raise ParentNotFoundError(err.detail or '')

    @abstractmethod
    def _get_new_nodes_records(self, import_id: int) -> list[dict[str, Any]]:
        """new nodes db dicts"""

    async def update_existent(self, import_id: int):
        if self.existent_ids:

            nodes_gen = (
                self.nodes[i].db_dict(import_id)
                for i in self.existent_ids
            )

            rows = [
                [node[key] for key in self.field_mapping]
                for node in nodes_gen
            ]

            try:
                await self.conn.executemany(self.Query.update_many(self.field_mapping), rows)
            except ForeignKeyViolationError as err:
                raise ParentNotFoundError(err.detail or '')


class FileListRepository(ItemListBaseRepository):
    NodeT = ItemType.FILE
    Query = FileQuery
    field_mapping = {
        key: f'${i}'
        for i, key in enumerate(RequestItem.db_fields_set(NodeT) | {'import_id'}, start=1)
    }

    __slots__ = ()

    def _get_new_nodes_records(self, import_id: int):
        return [
            self.nodes[i].db_dict(import_id)
            for i in self.new_ids
        ]

    async def write_history(self):
        if self.existent_ids:
            select_q = self.Query.select(self.existent_ids)
            insert_hist = self.Query.insert_history_from_select(select_q)

            await self.conn.execute(insert_hist)


class FolderListRepository(ItemListBaseRepository):
    NodeT = ItemType.FOLDER
    Query = FolderQuery
    field_mapping = {
        key: f'${i}'
        for i, key in enumerate(RequestItem.db_fields_set(NodeT) | {'import_id'}, start=1)
    }

    __slots__ = ()

    def _get_new_nodes_records(self, import_id: int):
        new_folders = (self.nodes[i] for i in self.new_ids)
        folder_trees = RequestNodeTree.from_nodes(new_folders)
        ordered_folders = sum((tree.flatten_nodes(import_id) for tree in folder_trees), [])
        return ordered_folders
