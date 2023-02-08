from datetime import datetime
from typing import Iterable

from asyncpgsa.connection import SAConnection

from cloud.db.queries import import_queries, FolderQuery, Ids
from cloud.utils import QueueWorker
from .base import BaseRepository


class ImportRepository(BaseRepository):
    __slots__ = ('_import_id',)

    def __init__(self, conn: SAConnection, import_id: int | None = None):
        super().__init__(conn)

        self._import_id = import_id

    @property
    def import_id(self) -> int:
        return self._import_id

    @import_id.setter
    def import_id(self, value: int):
        self._import_id = value

    # note: not used
    async def insert_import_auto_id(self, date: datetime):
        self._import_id = await self.conn.fetchval(
            import_queries.insert_import_auto_id(date))

    async def insert_import(self, date: datetime):
        await self.conn.execute(
            import_queries.insert_import(self.import_id, date))

    def release_queue(self):
        QueueWorker.release_queue(self.import_id)

    async def lock_ids(self, ids: Iterable[str]):
        query = 'VALUES '
        query += ', '.join(
            f"(pg_advisory_xact_lock(hashtextextended('{i}', 0)))"
            for i in ids
        )
        await self.conn.execute(query)

    async def lock_branches(self, folder_ids: Ids, file_ids: Ids):
        """locks old and new parent branches for given ids"""

        cte = import_queries.folders_with_recursive_parents_cte(
            folder_ids,
            file_ids,
            ['id', 'parent_id']
        )
        await self.conn.execute(
            import_queries.lock_ids_from_select(cte)
        )

    def acquire_locks_ctx(self, ids: Iterable[str]):
        return AcquireLocksContext(self, ids)

    async def write_folders_history(self, folder_ids, file_ids):
        """
        Write in folder_history table folder records:
            1) with id in folder_ids
            2) all recursive parents of folders with id in folder_ids
            3) all recursive parents of files with id in file_ids
        All records selecting in one recursive query.
        """
        cte = import_queries.folders_with_recursive_parents_cte(
            folder_ids, file_ids
        )
        insert_hist = FolderQuery.insert_history_from_select(cte.select())
        await self.conn.execute(insert_hist)

    async def subtract_parent_sizes(self, folders_existent_ids: Ids, files_existent_ids: Ids):
        if folders_existent_ids or files_existent_ids:
            query = import_queries.update_parent_sizes(
                files_existent_ids,
                folders_existent_ids,
                self.import_id,
                import_queries.Sign.SUB
            )
            await self.conn.execute(query)

    async def add_parent_sizes(self, file_ids: Ids, folder_ids: Ids):
        query = import_queries.update_parent_sizes(
            file_ids,
            folder_ids,
            self.import_id
        )
        await self.conn.execute(query)


class AcquireLocksContext:
    __slots__ = ('mdl', 'i', '_ids', '_branches_ids')

    def __init__(self, mdl: ImportRepository, ids: Ids, i: int = 0):
        self.mdl = mdl
        self.i = i
        self._ids = ids
        self._branches_ids = None

    @property
    def branches_leaf_ids(self) -> tuple[Ids, Ids]:
        return self._branches_ids

    @branches_leaf_ids.setter
    def branches_leaf_ids(self, value: tuple[Ids, Ids]):
        """folder ids, file ids"""
        self._branches_ids = value

    async def __aenter__(self):
        await self.mdl.conn.execute('SELECT pg_advisory_lock($1)', self.i)
        await self.mdl.lock_ids(self._ids)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                await self.mdl.lock_branches(*self.branches_leaf_ids)
        finally:
            await self.mdl.conn.execute('SELECT pg_advisory_unlock($1)', self.i)
