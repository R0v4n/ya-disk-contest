from __future__ import annotations

from abc import ABC
from functools import reduce

from asyncpgsa import PG, compile_query
from asyncpgsa.connection import SAConnection
from sqlalchemy import Table, bindparam

from cloud.api.model.data_classes import ImportData, File, Folder, Node
from cloud.api.model.folder_tree import NodeTree
from cloud.api.model.query_builder import FileQuery, FolderQuery, QueryBase, ImportQuery
from cloud.db.schema import folders, files, file_history, folder_history
from cloud.utils.pg import DEFAULT_PG_URL
from .base_model import BaseModel


class ImportModel(BaseModel):

    def __init__(self, data: ImportData, conn: SAConnection):

        self.data = data
        super().__init__(conn, data.date)

        self.files_mdl: FileListModel | None = None
        self.folders_mdl: FolderListModel | None = None
        self.queries: ImportQuery | None = None

    async def init(self):
        await self.insert_import()

        self.folders_mdl = FolderListModel(self.data, self.import_id, self.conn)
        await self.folders_mdl.init()

        self.files_mdl = FileListModel(self.data, self.import_id, self.conn)
        await self.files_mdl.init()

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
            await self.conn.execute(self.queries.update_folders_size(add=False))

    async def write_files_history(self):
        if self.files_mdl.exist_ids:
            select_q = FileQuery.select(self.files_mdl.exist_ids)
            await self.files_mdl.write_history(select_q)

    async def insert_new_nodes(self):
        # todo: what about race???
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
        query = ImportQuery(self.files_mdl.ids, self.folders_mdl.ids, self.import_id).update_folders_size()

        await self.conn.execute(query)

    async def just_do_it(self):
        await self.write_folders_history()
        await self.write_files_history()

        await self.subtract_parents_size()

        await self.insert_new_nodes()
        await self.update_existing_nodes()

        await self.add_parents_size()


class NodeListBaseModel(ABC):
    NodeClass: type[Node]
    table: Table
    history_table: Table
    Query: type[QueryBase]

    def __init__(self, data: ImportData, import_id: int,
                 conn: SAConnection):
        self.import_id = import_id
        self.conn = conn
        self.date = data.date

        self.nodes = [i for i in data.items if type(i) == self.NodeClass]
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

    # async def get_new_ids(self):
    #     return self.ids - (await self.existing_ids)

    async def insert_new(self):
        await self.conn.execute(self.Query.insert(self._get_new_nodes_values()))

    # todo: move to file class
    def _get_new_nodes_values(self):
        return [
            node.dict() | {'import_id': self.import_id}
            for node in self.nodes
            if node.id in self.new_ids
        ]

    async def update_existing(self):
        # todo: need refactor
        mapping = {key: f'${i}' for i, key in enumerate(self.NodeClass.__fields__.keys() | {'import_id'}, start=1)}

        rows = [[(node.dict() | {'import_id': self.import_id})[key] for key in mapping] for node in self.nodes if
                node.id in self.exist_ids]

        id_param = mapping.pop('id')

        cols = ', '.join(f'{key}={val}' for key, val in mapping.items())

        query = f'UPDATE {self.table.name} SET {cols} WHERE id = {id_param}'
        await self.conn.executemany(query, rows)

    # todo: not used. remove or refactor
    async def update_existing1(self):
        # todo: move to Query?
        bp_dict = {key: bindparam(key + '_') for key in self.NodeClass.__fields__} | {
            'import_id': bindparam('import_id_')}

        # print(bp_dict)
        # {k + '_': v for k, v in node.dict()}
        rows = [{k: v for k, v in node.dict().items()} | {'import_id': self.import_id} for node in self.nodes if
                node.id in self.exist_ids]
        # rows = [list(d.values()) for d in rows]
        # print(rows)
        query = self.Query.update(bp_dict)
        # exit()
        #
        a, b = compile_query(query)
        # print(query)
        # a = 'UPDATE files SET import_id=$5, parent_id=$2, url=$3, size=$4 WHERE files.id = $1'
        # # print(rows)
        # # self.conn.
        # print(await self.conn.executemany(a, rows))

    async def write_history(self, select_query):
        insert_hist = self.Query.insert_history_from_select(select_query)

        await self.conn.execute(insert_hist)


class FileListModel(NodeListBaseModel):
    NodeClass = File
    table = files
    history_table = file_history
    Query = FileQuery


class FolderListModel(NodeListBaseModel):
    NodeClass = Folder
    table = folders
    history_table = folder_history
    Query = FolderQuery

    def _get_new_nodes_values(self):
        new_folders = (node for node in self.nodes if node.id in self.new_ids)

        folder_trees = NodeTree.from_nodes(new_folders)

        ordered_folders = sum((list(tree.flatten_nodes_dict_gen()) for tree in folder_trees), [])

        return [node_dict | {'import_id': self.import_id} for node_dict in ordered_folders]


if __name__ == '__main__':
    import asyncio
    from unit_test import IMPORT_BATCHES, UPDATE_IMPORT


    async def some_new_imports():
        pg = PG()
        await pg.init(DEFAULT_PG_URL)

        async with pg.transaction() as conn:
            for i, batch in enumerate(IMPORT_BATCHES):
                data = ImportData(**batch)
                mdl = ImportModel(data, conn)
                await mdl.init()
                await mdl.just_do_it()


    async def update_import():
        pg = PG()
        await pg.init(DEFAULT_PG_URL)

        async with pg.transaction() as conn:
            data = ImportData(**UPDATE_IMPORT)

            mdl = ImportModel(data, conn)
            await mdl.init()
            await mdl.just_do_it()


    async def main():
        await some_new_imports()
        # await update_import()


    asyncio.run(main())
