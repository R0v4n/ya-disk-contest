import asyncio
from bisect import insort_right
from datetime import datetime
from functools import partial
from itertools import islice
from random import sample
from time import monotonic

import pytest
from asyncpgsa import PG
from devtools import debug

from cloud.api.app import create_app
from cloud.api.handlers import ImportsView, DeleteNodeView
from cloud.api.model import ImportModel, ImportData, NodeModel
from cloud.utils.testing import (post_import, del_node,
                                 compare_db_fc_state, get_node, File, FakeCloudGen, random_schema)


class QueueImportModel(ImportModel):
    queue: list[datetime] = []

    def __init__(self, data: ImportData, pg: PG):
        insort_right(self.queue, data.date)
        super().__init__(data, pg)

    async def acquire_lock(self):
        await self.conn.execute('SELECT pg_advisory_xact_lock($1)', 0)

    async def execute_post_import(self):
        await super().execute_post_import()
        self.queue.pop(0)

    # async def init(self):
    #     await self.acquire_lock()
    #     await super().init()

    async def init(self):
        for _ in range(1000000):
            if self.queue[0] == self.date:
                # await self.acquire_lock()
                await super().init()
                break

            await asyncio.sleep(0.01)

        else:
            raise ConnectionError


class QueueImportModel2(ImportModel):

    async def init(self):
        for _ in range(1000000):
            if self.queue[0] == self.date:
                # await self.acquire_lock()
                await super().init()
                break

            await asyncio.sleep(0.01)

        else:
            raise ConnectionError


class QueueNodeModel(NodeModel):
    async def _delete_node(self):
        for _ in range(1000000):
            if self.queue[0] == self.date:
                # await self.acquire_lock()
                await super()._delete_node()
                break

            await asyncio.sleep(0.01)

        else:
            raise ConnectionError


class LockImportModel(ImportModel):

    async def acquire_lock(self):
        await self.conn.execute('SELECT pg_advisory_xact_lock($1)', 0)

    async def execute_post_import(self):
        await super().execute_post_import()

    async def init(self):
        # await self.acquire_lock()
        await super().init()


class QueueImportsView(ImportsView):
    URL_PATH = r'/with_lock/imports'
    ModelT = QueueImportModel2


class QueueDeleteNodeView(DeleteNodeView):
    URL_PATH = r'/with_lock/delete/{node_id}'
    ModelT = QueueNodeModel


# fixme: this is a stub for the future
class PatchedImportsViewWithoutLock(QueueImportsView):
    URL_PATH = r'/no_lock/imports'


@pytest.fixture
async def api_client(aiohttp_client, arguments, migrated_postgres):
    """
    Добавляем измененные обработчики в сервис. aiohttp требуется создать заново
    (т.к. изменять набор обработчиков после запуска не разрешено).
    """
    app = create_app(arguments)
    app.router.add_route('*', QueueImportsView.URL_PATH, QueueImportsView)
    app.router.add_route('*', QueueDeleteNodeView.URL_PATH, QueueDeleteNodeView)

    client = await aiohttp_client(app, server_kwargs={
        'port': arguments.api_port
    })

    try:
        yield client
    finally:
        await client.close()


async def test_several_file_update_with_model(api_client, fake_cloud, postgres, sync_connection):
    fake_cloud.generate_import()
    f1 = File(size=10)
    fake_cloud.insert_item(f1)

    c = 100
    for i in range(c):
        fake_cloud.generate_import()
        fake_cloud.update_item(f1.id, size=f1.size + i)

    data_list = [ImportData(**data) for data in fake_cloud.imports_gen()]

    pg = PG()
    await pg.init(postgres)

    async def post(data):
        model = QueueImportModel(data, pg)
        await model.execute_post_import()

    # for data in data_list:
    #     await post(data)

    await asyncio.gather(*[post(data) for data in data_list])

    compare_db_fc_state(sync_connection, fake_cloud)

# fixme: this tests falls when running all test and works when runnning only this module. wtf?
# async def test_several_file_update_with_client(api_client, fake_cloud, sync_connection):
#     def make_imports(count=10):
#         fake_cloud.generate_import()
#         f1 = File(size=10)
#         fake_cloud.insert_item(f1)
#
#         for i in range(count - 1):
#             fake_cloud.generate_import()
#             fake_cloud.update_item(f1.id, size=f1.size + i)
#
#         return fake_cloud.imports_gen()
#
#     imports = make_imports(10)
#
#     # for data in imports:
#     #     await post_import(api_client, data, url=QueueImportsView.URL_PATH)
#
#     corus = [post_import(api_client, data, url=QueueImportsView.URL_PATH) for data in imports]
#
#     t0 = monotonic()
#     await asyncio.gather(*corus)
#     print(monotonic() - t0)
#
#     compare_db_fc_state(sync_connection, fake_cloud)
#
#
# def batched(iterable, n):
#     """Batch data into lists of length n. The last batch may be shorter."""
#     it = iter(iterable)
#     while batch := list(islice(it, n)):
#         yield batch
#
#
# async def test_post_and_delete(api_client, sync_connection):
#     cloud = FakeCloudGen()
#     each_import_ids = []
#
#     def make_imports(count=10):
#         cloud.generate_import([[[[[[[[[[[]]]]]]]]]]])
#         each_import_ids.append(cloud.ids)
#         schema_gen = partial(random_schema, max_files_in_one_folder=2)
#
#         for _ in range((count - 1) // 2):
#             cloud.random_import(schemas_count=10, schema_gen_func=schema_gen, allow_random_count=False)
#             cloud.random_updates(count=5, allow_random_count=False)
#             each_import_ids.append(cloud.ids)
#
#             cloud.random_del()
#             each_import_ids.append(cloud.ids)
#
#         if count % 2 == 0:
#             cloud.random_import(schemas_count=10, schema_gen_func=schema_gen, allow_random_count=False)
#             each_import_ids.append(cloud.ids)
#
#         return cloud.imports_gen()
#
#     async def gather_requests(reqs_corus):
#         # t0 = monotonic()
#         res = await asyncio.gather(*reqs_corus, return_exceptions=True)
#         # res = [await c for c in reqs_corus]
#         # dt = monotonic() - t0
#         # debug(dt)
#         # debug(res)
#         return res
#
#     def post_and_del_corus(imports):
#         return [
#             post_import(api_client, data, url=QueueImportsView.URL_PATH)
#             if 'items' in data else
#             del_node(api_client, data['deleted_id'], data['updateDate'], url=QueueDeleteNodeView.URL_PATH)
#
#             for data in imports
#         ]
#
#     def node_corus(ids, k=10):
#         if k > len(ids):
#             k = len(ids)
#
#         return [get_node(api_client, i) for i in sample(ids, k)]
#
#     ids = []
#     batch_len = 3
#
#     t0 = monotonic()
#     imports = make_imports(50)
#     t1 = monotonic()
#     debug(t1 - t0)
#     for i, batch in enumerate(batched(imports, batch_len)):
#         if i != 0:
#             ids = each_import_ids[i * batch_len - 1]
#         await gather_requests(post_and_del_corus(batch) + node_corus(ids, batch_len * 6))
#
#     dt = monotonic() - t1
#     debug(dt)
#
#     compare_db_fc_state(sync_connection, cloud)

