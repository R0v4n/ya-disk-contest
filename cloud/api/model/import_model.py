from abc import ABC, abstractmethod
from functools import reduce
from typing import Iterable, TypeVar, Any

from aiohttp.web_exceptions import HTTPBadRequest
from asyncpg import ForeignKeyViolationError
from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from .base_model import BaseImportModel
from .data_classes import ImportData, ItemType, ImportItem, ParentIdValidationError
from .node_tree import ImportNodeTree
from .query_builder import FileQuery, FolderQuery, QueryBase


QueryT = TypeVar('QueryT', bound=QueryBase)


# todo: add slots to models. check private methods everywhere
class ImportModel(BaseImportModel):

    def __init__(self, data: ImportData, pg: PG):

        self.data = data
        super().__init__(pg, data.date)

        self.files_mdl: FileListModel | None = None
        self.folders_mdl: FolderListModel | None = None

    async def init(self):
        await self.insert_import()

        self.folders_mdl = FolderListModel(self.data, self.import_id, self.conn)
        await self.folders_mdl.init()

        self.files_mdl = FileListModel(self.data, self.import_id, self.conn)
        await self.files_mdl.init()

        if await self.files_mdl.any_id_exists(self.folders_mdl.ids) \
                or await self.folders_mdl.any_id_exists(self.files_mdl.ids):
            raise HTTPBadRequest

    # todo: create BaseHistoryModel
    async def write_folders_history(self):
        """
        write in folder_history table folder records that will be updated during import:
            1) new nodes existent parents
            2) current parents for updating nodes
            3) new existent parents for updating nodes
            4) existing folders in import
        """
        # existent parents for updating folders will be unioned in select query

        # all folders and their new direct parents (4), (3)
        ids_set = reduce(set.union, ({i.id, i.parent_id} for i in self.folders_mdl.nodes), set())
        # union files new parents (3)
        ids_set |= {i.parent_id for i in self.files_mdl.nodes}
        # union files old parents (1), (2)
        ids_set |= self.files_mdl.existent_parent_ids
        # diff new folder
        ids_set -= self.folders_mdl.new_ids
        ids_set -= {None}

        if ids_set:
            folders_select = FolderQuery.select(ids_set)
            parents = FolderQuery.recursive_parents(ids_set)
            await self.folders_mdl.write_history(folders_select.union(parents))

    async def subtract_parents_size(self):
        if self.files_mdl.existent_ids or self.folders_mdl.existent_ids:
            await self.conn.execute(
                FolderQuery.update_parent_sizes(
                    self.files_mdl.existent_ids,
                    self.folders_mdl.existent_ids,
                    self.import_id,
                    False
                )
            )

    async def write_files_history(self):
        if self.files_mdl.existent_ids:
            select_q = FileQuery.select(self.files_mdl.existent_ids)
            await self.files_mdl.write_history(select_q)

    async def insert_new_nodes(self):
        if self.folders_mdl.new_ids:
            await self.folders_mdl.insert_new()
        if self.files_mdl.new_ids:
            await self.files_mdl.insert_new()

    async def update_existing_nodes(self):
        if self.folders_mdl.existent_ids:
            await self.folders_mdl.update_existing()
        if self.files_mdl.existent_ids:
            await self.files_mdl.update_existing()

    async def add_parents_size(self):
        await self.conn.execute(
            FolderQuery.update_parent_sizes(
                self.files_mdl.ids,
                self.folders_mdl.ids,
                self.import_id
            )
        )

    async def execute_post_import(self):
        await self.execute_in_transaction(
            self.init,
            self.write_folders_history,
            self.write_files_history,
            self.subtract_parents_size,
            self.insert_new_nodes,
            self.update_existing_nodes,
            self.add_parents_size
        )


class NodeListBaseModel(ABC):
    # todo: add hasattr check. rename to factory? also with Query
    NodeT: ItemType
    Query: type[QueryT]

    def __init__(self, data: ImportData, import_id: int,
                 conn: SAConnection):
        self.import_id = import_id
        self.conn = conn
        self.date = data.date

        self.nodes = [i for i in data.items if i.type == self.NodeT]
        self.ids = {node.id for node in self.nodes}
        self.new_ids = None
        self.existent_ids = None
        self.existent_parent_ids = None

    async def init(self):
        self.existent_ids, self.existent_parent_ids = await self.get_existing_ids()
        self.new_ids = self.ids - self.existent_ids

    async def get_existing_ids(self) -> tuple[set[str], set[str]]:
        query = self.Query.select(self.ids, ['id', 'parent_id'])

        res = await self.conn.fetch(query)
        return reduce(
            lambda s, x: (s[0] | {x['id']}, s[1] | {x['parent_id']}),
            res,
            (set(), set())
        )

    async def any_id_exists(self, ids: Iterable[str]):
        return await self.conn.fetchval(self.Query.exist(ids))

    async def insert_new(self):
        try:
            await self.conn.execute(self.Query.insert(self._get_new_nodes_records()))
        except ForeignKeyViolationError as err:
            raise ParentIdValidationError(err.detail or '')

    @abstractmethod
    def _get_new_nodes_records(self) -> list[dict[str, Any]]:
        """new nodes db dicts"""

    async def update_existing(self):
        mapping = {key: f'${i}' for i, key in enumerate(ImportItem.db_fields_set(self.NodeT) | {'import_id'}, start=1)}

        rows = [[(node.db_dict(self.import_id))[key] for key in mapping] for node in self.nodes if
                node.id in self.existent_ids]

        try:
            await self.conn.executemany(self.Query.update_many(mapping), rows)
        # fixme: this can be a problem if FK error will be raised due to node id.
        except ForeignKeyViolationError as err:
            raise ParentIdValidationError(err.detail or '')

    async def write_history(self, select_query):
        insert_hist = self.Query.insert_history_from_select(select_query)

        await self.conn.execute(insert_hist)


class FileListModel(NodeListBaseModel):
    NodeT = ItemType.FILE
    Query = FileQuery

    def _get_new_nodes_records(self):
        return [
            node.db_dict(self.import_id)
            for node in self.nodes
            if node.id in self.new_ids
        ]


class FolderListModel(NodeListBaseModel):
    NodeT = ItemType.FOLDER
    Query = FolderQuery

    def _get_new_nodes_records(self):
        new_folders = (node for node in self.nodes if node.id in self.new_ids)

        folder_trees = ImportNodeTree.from_nodes(new_folders)

        ordered_folders = sum((list(tree.flatten_nodes_dict_gen(self.import_id)) for tree in folder_trees), [])

        return ordered_folders
