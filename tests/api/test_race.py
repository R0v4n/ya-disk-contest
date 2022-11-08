from bisect import insort_right
from collections import deque
from datetime import timedelta, datetime, timezone
from http import HTTPStatus
import asyncio
from random import randint

import pytest
from asyncpgsa import PG
from asyncpgsa.connection import SAConnection
from deepdiff import DeepDiff
from devtools import debug

from cloud.api.app import create_app
from cloud.api.handlers import ImportsView
from cloud.api.model import ImportModel, ImportData
from cloud.utils.testing import post_import, FakeCloud, compare_db_fc_state, Folder, get_node, File


class PatchedImportModel(ImportModel):
    queue: list[datetime] = []

    def __init__(self, data: ImportData, conn: SAConnection):
        super().__init__(data, conn)
        # insort_right(self.queue, self.date)

    async def acquire_lock(self):
        await self.conn.execute('SELECT pg_advisory_xact_lock($1)', 0)

    async def just_do_it(self):
        await super().just_do_it()

    async def init(self):
        await self.acquire_lock()
        await super().init()


    # async def init(self):
    #     debug(self.queue)
    #     if self.queue[0] == self.date:
    #         await self.acquire_lock()
    #         await super().init()
    #         self.queue.pop(0)
    #
    #     else:
    #         await asyncio.sleep(0.1)
    #         await self.init()


class PatchedImportsView(ImportsView):
    URL_PATH = r'/with_lock/imports'
    ModelT = PatchedImportModel


# fixme: this is a stub for the future
class PatchedImportsViewWithoutLock(PatchedImportsView):
    URL_PATH = r'/no_lock/imports'

    # @staticmethod
    # async def acquire_lock(conn, import_id):
    #     """
    #     Отключаем блокировку для получения состояния гонки.
    #     """

    @staticmethod
    async def acquire_lock(conn, import_id):
        await conn.execute('SELECT pg_advisory_xact_lock($1)', import_id)


@pytest.fixture
async def api_client(aiohttp_client, arguments, migrated_postgres):
    """
    Добавляем измененные обработчики в сервис. aiohttp требуется создать заново
    (т.к. изменять набор обработчиков после запуска не разрешено).
    """
    app = create_app(arguments)
    app.router.add_route('*', PatchedImportsView.URL_PATH, PatchedImportsView)

    client = await aiohttp_client(app, server_kwargs={
        'port': arguments.api_port
    })

    try:
        yield client
    finally:
        await client.close()


async def test(api_client, fake_cloud, postgres, sync_connection):
    fake_cloud.generate_import()
    f1 = File(size=10)
    fake_cloud.insert_item(f1)

    # await post_import(api_client, fake_cloud.get_import_dict())

    c = 2
    for i in range(1, c + 1):
        fake_cloud.generate_import()
        fake_cloud.update_item(f1.id, size=f1.size + i)

    imports = [fake_cloud.get_import_dict(i) for i in range(0, c + 1)]
    data_list = [ImportData(**data) for data in imports]

    pg = PG()
    await pg.init(postgres)

    i = 0

    async def post(data):
        nonlocal i
        if i:
            await asyncio.sleep(i)
        i += 0.05

        async with pg.transaction() as conn:
            model = PatchedImportModel(data, conn)
            await model.init()
            await model.just_do_it()

    await asyncio.gather(*[post(data) for data in data_list])

    # await post_import(api_client, fake_cloud.get_import_dict(-2))
    # await post_import(api_client, fake_cloud.get_import_dict(-1))

    # res = await asyncio.gather(*[post_import(api_client, data, url=PatchedImportsView.URL_PATH) for data in imports])
    # debug(res)
    compare_db_fc_state(sync_connection, fake_cloud)
