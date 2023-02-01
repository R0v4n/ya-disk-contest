from functools import reduce

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from .base_service import BaseImportService
from .exceptions import ModelValidationError
from .import_model import ImportModel
from .item_list_model import FileListModel, FolderListModel
from .schemas import RequestImport
from cloud.utils import QueueWorker
from ..db.schema import ItemType


# todo: refactor connection
# todo: clean WHERE 1!=1
class ImportService(BaseImportService):
    __slots__ = ('data', '_files_mdl', '_folders_mdl', '_folder_ids_set')

    def __init__(self, pg: PG, data: RequestImport):

        super().__init__(pg, data.date)
        self.data = data

        self._folders_mdl = None
        self._files_mdl = None
        self._folder_ids_set = None

    @property
    def folders_mdl(self) -> FolderListModel:
        return self._folders_mdl

    @property
    def files_mdl(self) -> FileListModel:
        return self._files_mdl

    @property
    def folder_ids_set(self) -> set[str]:
        """All folder ids from import items id and parent_id fields diff None parent_id"""
        # functools.cached_property doesn't work with __slots__
        if self._folder_ids_set is None:
            self._folder_ids_set = reduce(
                set.union,
                (
                    {i.id, i.parent_id} if i.type == ItemType.FOLDER else {i.parent_id}
                    for i in self.data.items
                ),
                set()
            )
            self._folder_ids_set -= {None}

        return self._folder_ids_set

    def create_models(self, conn: SAConnection):
        files = {}
        folders = {}

        for item in self.data.items:
            if item.type == ItemType.FOLDER:
                folders[item.id] = item
            else:
                files[item.id] = item

        self._files_mdl = FileListModel(conn, files)
        self._folders_mdl = FolderListModel(conn, folders)
        self._import_mdl = ImportModel(conn, self._files_mdl.ids, self.folder_ids_set)

    async def init_models(self):
        await super().init_models()

        await self.folders_mdl.init()
        await self.files_mdl.init()

        if await self.files_mdl.any_id_exists(self.folders_mdl.ids):
            raise ModelValidationError('Some folder ids already exists in files ids')
        if await self.folders_mdl.any_id_exists(self.files_mdl.ids):
            raise ModelValidationError('Some file ids already exists in folders ids')

    async def _post_import(self):
        if self.data.items:
            import_id = self.import_mdl.import_id

            # write history
            await self.import_mdl.write_folders_history()
            await self.files_mdl.write_history()

            await self.import_mdl.subtract_parent_sizes(
                self.files_mdl.existent_ids,
                self.folders_mdl.existent_ids
            )
            # insert new nodes
            await self.folders_mdl.insert_new(import_id),
            await self.files_mdl.insert_new(import_id),
            # update existent nodes
            await self.folders_mdl.update_existent(import_id),
            await self.files_mdl.update_existent(import_id),

            await self.import_mdl.add_parent_sizes()

    async def execute_post_import(self):
        await self._execute_in_import_transaction(self._post_import())

