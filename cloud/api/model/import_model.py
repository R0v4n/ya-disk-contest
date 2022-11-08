from __future__ import annotations

from abc import ABC
from functools import reduce
from typing import Iterable

from aiohttp.web_exceptions import HTTPBadRequest
from asyncpg import ForeignKeyViolationError
from asyncpgsa.connection import SAConnection
from sqlalchemy import Table

from cloud.api.model.data_classes import ImportData, NodeType, ImportNode, ParentIdValidationError
from cloud.api.model.node_tree import ImportNodeTree
from cloud.api.model.query_builder import FileQuery, FolderQuery, QueryBase, ImportQuery
from cloud.db.schema import folders_table, files_table
from .base_model import BaseModel


class ImportModel(BaseModel):

    def __init__(self, data: ImportData, pg: SAConnection):

        self.data = data
        super().__init__(pg, data.date)

        self.files_mdl: FileListModel | None = None
        self.folders_mdl: FolderListModel | None = None
        self.queries: ImportQuery | None = None

    async def init(self):
        await self.insert_import()

        self.folders_mdl = FolderListModel(self.data, self.import_id, self.conn)
        await self.folders_mdl.init()

        self.files_mdl = FileListModel(self.data, self.import_id, self.conn)
        await self.files_mdl.init()

        if await self.files_mdl.any_id_exists(self.folders_mdl.ids) \
                or await self.folders_mdl.any_id_exists(self.files_mdl.ids):
            raise HTTPBadRequest

        self.queries = ImportQuery(self.files_mdl.exist_ids, self.folders_mdl.exist_ids, self.import_id)

    # todo: create BaseHistoryModel
    async def write_folders_history(self):
        # todo: this could be done in queries
        # all folders and their strict parents
        ids_set = reduce(set.union, ({i.id, i.parent_id} for i in self.folders_mdl.nodes), set())

        # union files parents
        ids_set |= {i.parent_id for i in self.files_mdl.nodes} - {None}

        if ids_set:
            query = self.queries.recursive_parents(ids_set)
            await self.folders_mdl.write_history(query)

    async def subtract_parents_size(self):
        if self.files_mdl.exist_ids or self.folders_mdl.exist_ids:
            await self.conn.execute(self.queries.update_folder_sizes(add=False))

    async def write_files_history(self):
        if self.files_mdl.exist_ids:
            select_q = FileQuery.select(self.files_mdl.exist_ids)
            await self.files_mdl.write_history(select_q)

    async def insert_new_nodes(self):
        if self.folders_mdl.new_ids:
            await self.folders_mdl.insert_new()
        if self.files_mdl.new_ids:
            await self.files_mdl.insert_new()

    async def update_existing_nodes(self):
        if self.folders_mdl.exist_ids:
            await self.folders_mdl.update_existing()
        if self.files_mdl.exist_ids:
            await self.files_mdl.update_existing()

    async def add_parents_size(self):
        # todo: do i need refactor?
        query = ImportQuery(self.files_mdl.ids, self.folders_mdl.ids, self.import_id).update_folder_sizes()

        await self.conn.execute(query)

    async def just_do_it(self):
        await self.write_folders_history()
        await self.write_files_history()

        await self.subtract_parents_size()

        await self.insert_new_nodes()
        await self.update_existing_nodes()

        await self.add_parents_size()


class NodeListBaseModel(ABC):
    # todo: add hasattr check. rename to factory? also with Query
    NodeT: NodeType
    # todo: refactor update query and remove table field
    table: Table
    Query: type[QueryBase]

    def __init__(self, data: ImportData, import_id: int,
                 conn: SAConnection):
        self.import_id = import_id
        self.conn = conn
        self.date = data.date

        self.nodes = [i for i in data.items if i.type == self.NodeT]
        self.ids = {node.id for node in self.nodes}
        self.new_ids = None
        self.exist_ids = None

    async def init(self):
        self.exist_ids = await self.get_existing_ids()
        self.new_ids = self.ids - self.exist_ids

    async def get_existing_ids(self):
        query = self.Query.select(self.ids, ['id'])
        res = await self.conn.fetch(query)

        return {row['id'] for row in res}

    async def any_id_exists(self, ids: Iterable[str]):
        return await self.conn.fetchval(self.Query.exist(ids))

    async def insert_new(self):
        try:
            await self.conn.execute(self.Query.insert(self._get_new_nodes_values()))
        except ForeignKeyViolationError as err:
            raise ParentIdValidationError(err.detail or '')

    # todo: move to file class
    def _get_new_nodes_values(self):
        return [
            node.db_dict(self.import_id)
            for node in self.nodes
            if node.id in self.new_ids
        ]

    async def update_existing(self):
        # todo: need refactor
        mapping = {key: f'${i}' for i, key in enumerate(ImportNode.db_fields_set(self.NodeT) | {'import_id'}, start=1)}

        rows = [[(node.db_dict(self.import_id))[key] for key in mapping] for node in self.nodes if
                node.id in self.exist_ids]

        id_param = mapping.pop('id')

        cols = ', '.join(f'{key}={val}' for key, val in mapping.items())

        query = f'UPDATE {self.table.name} SET {cols} WHERE id = {id_param}'

        try:
            await self.conn.executemany(query, rows)
        # fixme: this can be a problem if FK error will be raised due to node id.
        except ForeignKeyViolationError as err:
            raise ParentIdValidationError(err.detail or '')

    # todo: not used. remove or refactor
    async def update_existing1(self):
        # todo: move to Query?
        # bp_dict = {key: bindparam(key + '_') for key in self.NodeClass.__fields__} | {
        #     'import_id': bindparam('import_id_')}

        # print(bp_dict)
        # {k + '_': v for k, v in node.dict()}
        rows = [{k: v for k, v in node.dict().items()} | {'import_id': self.import_id} for node in self.nodes if
                node.id in self.exist_ids]
        # rows = [list(d.values()) for d in rows]
        # print(rows)
        # query = self.Query.update(bp_dict)
        # exit()
        #
        # a, b = compile_query(query)
        # print(query)
        # a = 'UPDATE files SET import_id=$5, parent_id=$2, url=$3, size=$4 WHERE files.id = $1'
        # # print(rows)
        # # self.conn.
        # print(await self.conn.executemany(a, rows))

    async def write_history(self, select_query):
        insert_hist = self.Query.insert_history_from_select(select_query)

        await self.conn.execute(insert_hist)


class FileListModel(NodeListBaseModel):
    NodeT = NodeType.FILE
    table = files_table
    Query = FileQuery


class FolderListModel(NodeListBaseModel):
    NodeT = NodeType.FOLDER
    table = folders_table
    Query = FolderQuery

    def _get_new_nodes_values(self):
        new_folders = (node for node in self.nodes if node.id in self.new_ids)

        folder_trees = ImportNodeTree.from_nodes(new_folders)

        ordered_folders = sum((list(tree.flatten_nodes_dict_gen(self.import_id)) for tree in folder_trees), [])

        return ordered_folders
