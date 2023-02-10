from functools import reduce

from asyncpgsa import PG
from asyncpgsa.connection import SAConnection

from disk.db.repositories import FileListRepository, FolderListRepository
from disk.models import RequestImport, ItemType
from .base import BaseImportService


class ImportService(BaseImportService):
    __slots__ = ('data', '_files_repo', '_folders_repo', '_folder_ids_set')

    def __init__(self, pg: PG, data: RequestImport):

        super().__init__(pg, data.date)
        self.data = data

        self._folders_repo = None
        self._files_repo = None
        self._folder_ids_set = None

    @property
    def folders_repo(self) -> FolderListRepository:
        return self._folders_repo

    @property
    def files_repo(self) -> FileListRepository:
        return self._files_repo

    @property
    def folder_ids_set(self) -> set[str]:
        """All folder ids from import items id and parent_id fields diff None parent_id"""
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

    def _create_items_repos(self, conn: SAConnection):
        files = {}
        folders = {}

        for item in self.data.items:
            if item.type == ItemType.FOLDER:
                folders[item.id] = item
            else:
                files[item.id] = item

        self._files_repo = FileListRepository(conn, files)
        self._folders_repo = FolderListRepository(conn, folders)

    async def acquire_locks(self, conn: SAConnection):
        await self.import_repo.lock_ids(self.folder_ids_set | self.files_repo.ids)
        await self.import_repo.lock_branches(self.folder_ids_set, self.files_repo.ids)
        self.import_repo.release_queue()

    async def init_repos(self, conn: SAConnection):
        if self.data.items:
            self._create_items_repos(conn)
            await self.acquire_locks(conn)

            await self.folders_repo.init()
            await self.files_repo.init()

            if self.folders_repo.ids:
                await self.files_repo.check_ids_not_exist(self.folders_repo.ids)

            if self.files_repo.ids:
                await self.folders_repo.check_ids_not_exist(self.files_repo.ids)

    async def write_history(self):
        """
        Write in folder_history table folder records that will be updated during import:
            1) new nodes existent parents
            2) old parents for updated nodes
            3) updated nodes new parents, that exist in the db
            4) updated folders (import items with type folder, that already exist in the db)
            5) all recursive parents of folders mentioned above

        All records selecting in one recursive query.

        Write in file_history table all file records that will be updated during import.
        """
        await self.import_repo.write_folders_history(
            self.folder_ids_set,
            self.files_repo.ids
        )
        await self.files_repo.write_history()

    async def _post_import(self):
        if self.data.items:
            import_id = self.import_repo.import_id

            await self.write_history()

            await self.import_repo.subtract_parent_sizes(
                self.folders_repo.existent_ids,
                self.files_repo.existent_ids
            )
            # insert new nodes
            await self.folders_repo.insert_new(import_id),
            await self.files_repo.insert_new(import_id),
            # update existent nodes
            await self.folders_repo.update_existent(import_id),
            await self.files_repo.update_existent(import_id),

            await self.import_repo.add_parent_sizes(
                self.files_repo.ids,
                self.folders_repo.ids
            )

    async def execute_post_import(self):
        await self._execute_in_import_transaction(self._post_import())
