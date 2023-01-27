from functools import reduce

from .base import BaseImportModel
from .exceptions import ModelValidationError
from .item_list_model import FileListModel, FolderListModel
from .queries import FolderQuery, import_queries
from .schemas import RequestImport
from cloud.utils.pg import advisory_lock


# todo: clean WHERE 1!=1
# Should this class be named ImportService?
class ImportModel(BaseImportModel):
    """Wraps FolderListModel and FileListModel interfaces in calls to init and execute_post_import methods"""

    __slots__ = ('data', 'files_mdl', 'folders_mdl', '_folder_ids_set')

    def __init__(self, data: RequestImport):

        self.data = data
        super().__init__(data.date)

        self.folders_mdl = FolderListModel(self.data)
        self.files_mdl = FileListModel(self.data)
        self._folder_ids_set = None

    async def init_sub_models(self):
        await self.folders_mdl.init(self.conn)
        self.folders_mdl.import_id = self.import_id
        await self.files_mdl.init(self.conn)
        self.files_mdl.import_id = self.import_id

        if await self.files_mdl.any_id_exists(self.folders_mdl.ids):
            raise ModelValidationError('Some folder ids already exists in files ids')
        if await self.folders_mdl.any_id_exists(self.files_mdl.ids):
            raise ModelValidationError('Some file ids already exists in folders ids')

    @property
    def folder_ids_set(self) -> set[str]:
        """All folder ids from import items id and parent_id fields diff None parent_id"""
        # functools.cached_property doesn't work with __slots__
        if self._folder_ids_set is None:
            ids = reduce(
                set.union,
                ({i.id, i.parent_id} for i in self.folders_mdl.nodes.values()),
                set()
            )
            ids |= {item.parent_id for item in self.files_mdl.nodes.values()}
            ids -= {None}
            self._folder_ids_set = ids

        return self._folder_ids_set

    async def acquire_ids_locks(self):
        """locks all updating folder branches by involved folder ids and also all import items ids"""
        async with advisory_lock(self.conn, 0):
            # todo: release queue here?
            await self.acquire_advisory_xact_lock_by_ids(
                self.files_mdl.ids | self.folder_ids_set
            )
            # todo: prepare statement?
            cte = import_queries.folders_with_recursive_parents_cte(
                self.folder_ids_set,
                self.files_mdl.ids,
                ['id', 'parent_id']
            )
            await self.conn.execute(
                import_queries.lock_ids_from_select(cte)
            )

    async def write_folders_history(self):
        """
        Write in folder_history table folder records that will be updated during import:
            1) new nodes existent parents
            2) old parents for updated nodes
            3) updated nodes new parents, that exist in the db
            4) updated folders (import items with type folder, that already exist in the db)
            5) all recursive parents of folders mentioned above

        All records selecting in one recursive query.
        """
        cte = import_queries.folders_with_recursive_parents_cte(
            self.folder_ids_set, self.files_mdl.ids
        )
        insert_hist = FolderQuery.insert_history_from_select(cte.select())
        await self.conn.execute(insert_hist)

    async def subtract_parent_sizes(self):
        if self.files_mdl.existent_ids or self.folders_mdl.existent_ids:
            query = import_queries.update_parent_sizes(
                self.files_mdl.existent_ids,
                self.folders_mdl.existent_ids,
                self.import_id,
                import_queries.Sign.SUB
            )
            await self.conn.execute(query)

    async def add_parent_sizes(self):
        query = import_queries.update_parent_sizes(
            self.files_mdl.ids,
            self.folders_mdl.ids,
            self.import_id
        )
        await self.conn.execute(query)

    async def execute_post_import(self):
        await self.wait_queue()

        if self.data.items:
            async with self.conn.transaction() as conn:
                self._conn = conn

                await self.acquire_ids_locks()
                await self.insert_import()
                await self.init_sub_models()

                # write history
                await self.write_folders_history()
                await self.files_mdl.write_history(self.files_mdl.existent_ids)

                await self.subtract_parent_sizes()
                # insert new nodes
                await self.folders_mdl.insert_new(),
                await self.files_mdl.insert_new(),
                # update existent nodes
                await self.folders_mdl.update_existent(),
                await self.files_mdl.update_existent(),

                await self.add_parent_sizes()
        else:
            await self.insert_import()


