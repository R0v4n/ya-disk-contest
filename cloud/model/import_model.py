from abc import ABC, abstractmethod
from functools import reduce
from typing import Iterable, Any

from asyncpg import ForeignKeyViolationError
from asyncpgsa.connection import SAConnection

from .base import BaseImportModel
from .exceptions import ParentNotFoundError, ModelValidationError
from .node_tree import RequestNodeTree
from .query_builder import FileQuery, FolderQuery, QueryT, Sign
from .schemas import RequestImport, ItemType, RequestItem


class ImportModel(BaseImportModel):
    """Wraps FolderListModel and FileListModel interfaces in calls to init and execute_post_import methods"""

    __slots__ = ('data', 'files_mdl', 'folders_mdl')

    def __init__(self, data: RequestImport):

        self.data = data
        super().__init__(data.date)

        self.files_mdl: FileListModel | None = None
        self.folders_mdl: FolderListModel | None = None

    async def init_sub_models(self):

        self.folders_mdl = FolderListModel(self.data, self.import_id, self.conn)
        await self.folders_mdl.init()

        self.files_mdl = FileListModel(self.data, self.import_id, self.conn)
        await self.files_mdl.init()

        if await self.files_mdl.any_id_exists(self.folders_mdl.ids):
            raise ModelValidationError('Some folder ids already exists in files ids')

        if await self.folders_mdl.any_id_exists(self.files_mdl.ids):
            raise ModelValidationError('Some file ids already exists in folders ids')

    async def write_folders_history(self):
        """
        Write in folder_history table folder records that will be updated during import:
            1) new nodes existent parents
            2) current parents for updated nodes
            3) updated nodes new parents, that exist in the db
            4) updated folders (import items with type folder, that already exist in the db)
            5) all recursive parents of folders mentioned above

        All records selecting in one recursive query.
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
        # diff "root id"
        ids_set -= {None}

        await self.folders_mdl.write_history(ids_set)

    def updated_folders_ids(self):
        # all folders and their new direct parents (4), (3)
        ids_set = reduce(set.union, ({i.id, i.parent_id} for i in self.folders_mdl.nodes), set())
        # union files new parents (3)
        ids_set |= {i.parent_id for i in self.files_mdl.nodes}
        # union files old parents (1), (2)
        ids_set |= self.files_mdl.existent_parent_ids
        # diff new folder
        ids_set -= self.folders_mdl.new_ids
        # diff "root id"
        ids_set -= {None}
        return ids_set

    async def execute_post_import(self):
        await self.queue_wait()

        async with self._conn.transaction() as conn:
            self._conn = conn
            await self.insert_import_with_model_id()
            await self.init_sub_models()
            ids = self.updated_folders_ids()
            if ids:
                await self.conn.execute(
                    self.folders_mdl.Query.lock_rows(
                        ids
                    ))
            await self.write_folders_history()
            # write files history
            await self.files_mdl.write_history(self.files_mdl.existent_ids)
            # subtract  parent sizes
            await self.folders_mdl.update_parent_sizes(
                self.files_mdl.existent_ids,
                self.folders_mdl.existent_ids,
                Sign.SUB
            )
            # insert new nodes
            await self.folders_mdl.insert_new(),
            await self.files_mdl.insert_new(),
            # update existent nodes
            await self.folders_mdl.update_existent(),
            await self.files_mdl.update_existent(),
            # add parent sizes
            await self.folders_mdl.update_parent_sizes(
                self.files_mdl.ids,
                self.folders_mdl.ids
            )


class NodeListBaseModel(ABC):
    NodeT: ItemType
    Query: type[QueryT]

    __slots__ = ('import_id', 'conn', 'date', 'nodes', 'ids',
                 '_new_ids', '_existent_ids', '_existent_parent_ids')

    def __init__(self, data: RequestImport, import_id: int,
                 conn: SAConnection):
        self.import_id = import_id
        self.conn = conn
        self.date = data.date

        self.nodes = [i for i in data.items if i.type == self.NodeT]
        self.ids = {node.id for node in self.nodes}
        self._new_ids = None
        self._existent_ids = None
        self._existent_parent_ids = None

    @property
    def new_ids(self) -> set[str]:
        return self._new_ids

    @property
    def existent_ids(self) -> set[str]:
        return self._existent_ids

    @property
    def existent_parent_ids(self) -> set[str]:
        return self._existent_parent_ids

    async def init(self):
        self._existent_ids, self._existent_parent_ids = await self.get_existing_ids()
        self._new_ids = self.ids - self.existent_ids

    async def get_existing_ids(self) -> tuple[set[str], set[str]]:
        query = self.Query.select(self.ids, ['id', 'parent_id'])

        return reduce(
            lambda s, x: (s[0] | {x['id']}, s[1] | {x['parent_id']}),
            await self.conn.fetch(query),
            (set(), set())
        )

    async def any_id_exists(self, ids: Iterable[str]):
        return await self.conn.fetchval(self.Query.exist(ids))

    async def insert_new(self):
        if self.new_ids:
            try:
                await self.conn.execute(self.Query.insert(self._get_new_nodes_records()))
            except ForeignKeyViolationError as err:
                raise ParentNotFoundError(err.detail or '')

    @abstractmethod
    def _get_new_nodes_records(self) -> list[dict[str, Any]]:
        """new nodes db dicts"""

    async def update_existent(self):
        if self.existent_ids:
            mapping = {
                key: f'${i}'
                for i, key in enumerate(RequestItem.db_fields_set(self.NodeT) | {'import_id'}, start=1)
            }

            rows = [
                [node.db_dict(self.import_id)[key] for key in mapping]
                for node in self.nodes
                if node.id in self.existent_ids
            ]

            try:
                await self.conn.executemany(self.Query.update_many(mapping), rows)
            # note: this can be a problem if FK error will be raised due to node id.
            except ForeignKeyViolationError as err:
                raise ParentNotFoundError(err.detail or '')

    @abstractmethod
    async def write_history(self, ids: Iterable[str]):
        """write records from node table to history table"""


class FileListModel(NodeListBaseModel):
    NodeT = ItemType.FILE
    Query = FileQuery

    __slots__ = ()

    def _get_new_nodes_records(self):
        return [
            node.db_dict(self.import_id)
            for node in self.nodes
            if node.id in self.new_ids
        ]

    async def write_history(self, ids: Iterable[str]):
        if ids:
            select_q = self.Query.select(ids)
            insert_hist = self.Query.insert_history_from_select(select_q)

            await self.conn.execute(insert_hist)


class FolderListModel(NodeListBaseModel):
    NodeT = ItemType.FOLDER
    Query = FolderQuery

    __slots__ = ()

    def _get_new_nodes_records(self):
        new_folders = (node for node in self.nodes if node.id in self.new_ids)

        folder_trees = RequestNodeTree.from_nodes(new_folders)

        ordered_folders = sum((list(tree.flatten_nodes_dict_gen(self.import_id)) for tree in folder_trees), [])

        return ordered_folders

    async def write_history(self, ids: Iterable[str]):
        """write  folder table records with given ids and all their recursive parents to history table"""
        if ids:
            # folders_select = self.Query.select(ids)
            # parents = self.Query.recursive_parents(ids)
            # insert_hist = self.Query.insert_history_from_select(folders_select.union(parents))
            # await self.conn.execute(insert_hist)
            q = self.Query.insert_history(ids)
            await self.conn.execute(q)

    async def update_parent_sizes(self, child_files_ids: Iterable[str],
                                  child_folders_ids: Iterable[str], sign: Sign = Sign.ADD):
        if child_files_ids or child_folders_ids:
            await self.conn.execute(
                self.Query.update_parent_sizes(
                    child_files_ids,
                    child_folders_ids,
                    self.import_id,
                    sign
                )
            )
