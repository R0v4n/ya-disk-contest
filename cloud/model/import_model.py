from asyncpgsa.connection import SAConnection

from cloud.model.base_model import BaseImportModel
from cloud.queries import import_queries, FolderQuery
from cloud.utils import advisory_lock, QueueWorker


class ImportModel(BaseImportModel):

    async def init(self):
        pass

    def __init__(self, conn: SAConnection, files_ids: set[str] = None, folders_ids: set[str] = None):
        super().__init__(conn)
        if not files_ids and not folders_ids:
            raise ValueError('Empty ids')

        self._files_ids = files_ids
        self._folders_ids = folders_ids

    async def acquire_ids_locks(self):
        query = 'VALUES '
        query += ', '.join(
            f"(pg_advisory_xact_lock(hashtextextended('{i}', 0)))"
            for i in self._files_ids | self._folders_ids
        )
        await self.conn.execute(query)

    async def lock_branches(self):
        """locks all updating folder branches by involved folder ids and also all import items ids"""
        # todo: try remove advisory_lock and release queue after locking ids
        async with advisory_lock(self.conn, 0):
            # todo: try release queue with pg
            QueueWorker.release_queue(self.import_id)

            await self.acquire_ids_locks()

            cte = import_queries.folders_with_recursive_parents_cte(
                self._folders_ids,
                self._files_ids,
                ['id', 'parent_id']
            )
            await self.conn.execute(
                import_queries.lock_ids_from_select(cte)
            )

    # todo: move description
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
            self._folders_ids, self._files_ids
        )
        insert_hist = FolderQuery.insert_history_from_select(cte.select())
        await self.conn.execute(insert_hist)

    async def subtract_parent_sizes(self, files_existent_ids, folders_existent_ids):
        if folders_existent_ids or files_existent_ids:
            query = import_queries.update_parent_sizes(
                files_existent_ids,
                folders_existent_ids,
                self.import_id,
                import_queries.Sign.SUB
            )
            await self.conn.execute(query)

    async def add_parent_sizes(self):
        query = import_queries.update_parent_sizes(
            self._files_ids,
            self._folders_ids,
            self.import_id
        )
        await self.conn.execute(query)
