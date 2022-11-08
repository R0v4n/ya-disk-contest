from bisect import insort_right
from collections import deque
from datetime import timedelta, datetime, timezone
from http import HTTPStatus
import asyncio
from random import randint, sample
from time import monotonic

import pytest
from asyncpgsa import PG
from asyncpgsa.connection import SAConnection
from deepdiff import DeepDiff
from devtools import debug

from cloud.api.app import create_app
from cloud.api.handlers import ImportsView
from cloud.api.model import ImportModel, ImportData, NodeType
from cloud.utils.testing import post_import, get_node_records, FakeCloud, compare_db_fc_state, Folder, get_node, File, \
    FakeCloudGen


class QueueImportModel(ImportModel):
    queue: list[datetime] = []

    def __init__(self, data: ImportData, pg: PG):
        insort_right(self.queue, data.date)
        super().__init__(data, pg)

    async def acquire_lock(self):
        await self.conn.execute('SELECT pg_advisory_xact_lock($1)', 0)

    # async def execute_post_import(self):
    #     for _ in range(10000):
    #         if self.queue[0] == self.date:
    #             await super().execute_post_import()
    #             self.queue.pop(0)
    #             break
    #
    #         await asyncio.sleep(0.01)
    #
    #     else:
    #         raise ConnectionError

    async def execute_post_import(self):
        await super().execute_post_import()
        self.queue.pop(0)

    # async def init(self):
    #     await self.acquire_lock()
    #     await super().init()

    async def init(self):
        for _ in range(1000000):
            if self.queue[0] == self.date:
                await self.acquire_lock()
                await super().init()
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
        await self.acquire_lock()
        await super().init()


class QueueImportsView(ImportsView):
    URL_PATH = r'/with_lock/imports'
    ModelT = QueueImportModel


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


async def test_several_file_update_with_client(api_client, fake_cloud, sync_connection):
    def make_imports(count=100):
        fake_cloud.generate_import()
        f1 = File(size=10)
        fake_cloud.insert_item(f1)

        for i in range(count - 1):
            fake_cloud.generate_import()
            fake_cloud.update_item(f1.id, size=f1.size + i)

        return fake_cloud.imports_gen()

    imports = make_imports(10)

    # for data in imports:
    #     await post_import(api_client, data, url=QueueImportsView.URL_PATH)

    corus = [post_import(api_client, data, url=QueueImportsView.URL_PATH) for data in imports]

    t0 = monotonic()
    await asyncio.gather(*corus)
    print(monotonic() - t0)

    compare_db_fc_state(sync_connection, fake_cloud)


async def test_get_node(api_client, sync_connection):
    cloud = FakeCloudGen()
    cloud.generate_import([[[[[[[[[[[]]]]]]]]]]])

    for _ in range(3):
        cloud.random_import(schemas_count=200, allow_random_count=False, max_files_in_one_folder=1)
        cloud.random_updates(count=100, allow_random_count=False)

    import_corus = [post_import(api_client, data, url=QueueImportsView.URL_PATH)
                    for data in cloud.imports_gen()]

    debug(len(cloud.ids))

    k = 3
    if k > len(cloud.ids):
        k = len(cloud.ids)

    node_corus = [get_node(api_client, i) for i in sample(cloud.ids, k)]

    async def gather_requests(reqs_corus):
        t0 = monotonic()
        res = await asyncio.gather(*reqs_corus, return_exceptions=True)
        # for c in node_corus:
        #     await c
        dt = monotonic() - t0
        debug(dt)

        return res

    debug(await gather_requests(import_corus))
    debug(await gather_requests(node_corus))

    compare_db_fc_state(sync_connection, cloud)
