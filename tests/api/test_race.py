import asyncio
from contextlib import nullcontext
from http import HTTPStatus

import pytest
from aiohttp.test_utils import TestServer, TestClient
from aiohttp.web_urldispatcher import UrlDispatcher
from asyncpg import UniqueViolationError
from asyncpgsa.connection import SAConnection
from fastapi import APIRouter
from httpx import AsyncClient
from starlette.responses import Response

from cloud import model
from cloud import services
from cloud.api_fastapi.app import create_app as create_fastapi_app
from cloud.api_fastapi.routers import service_depends
from cloud.api_aiohttp.app import create_app as create_aiohttp_app
from cloud.api_aiohttp.handlers import ImportsView, DeleteNodeView
from cloud.resources import url_paths
from cloud.utils.testing import (
    post_import, del_node,
    compare_db_fc_state, Folder, FakeCloudGen
)

delay_imports_url = '/delay' + url_paths.IMPORTS
no_locks_imports_url = '/no-locks' + url_paths.IMPORTS
no_queue_imports_url = '/no-queue' + url_paths.IMPORTS

delay_delete_node_url = '/delay' + url_paths.DELETE_NODE
no_locks_delete_node_url = '/no-locks' + url_paths.DELETE_NODE


class DelayNodeImportService(services.NodeImportService):
    async def _delete_node(self):
        await asyncio.sleep(0.5)
        await super()._delete_node()


class NoLocksNodeImportService(services.NodeImportService):

    async def init_models(self, conn: SAConnection):
        self._mdl = model.NodeModel(conn, self.node_id)
        await self.mdl.init()
        self.import_mdl.release_queue()


class NoQueueImportService(services.ImportService):
    import_id = 10000

    async def acquire_locks(self, conn: SAConnection):
        await self.import_mdl.lock_ids(self.folder_ids_set | self.files_mdl.ids)
        await self.import_mdl.lock_branches(self.folder_ids_set, self.files_mdl.ids)
        # self.import_mdl.release_queue()

    async def execute_post_import(self):
        async with self.pg.transaction() as conn:
            self._import_mdl = model.ImportModel(conn, self.import_id)
            self.__class__.import_id += 1

            await self.init_models(conn)
            await self.import_mdl.insert_import(self.date)
            await self._post_import()


class DelayImportService(services.ImportService):

    async def _post_import(self):
        await asyncio.sleep(0.5)
        await super()._post_import()


class NoLocksImportService(DelayImportService):

    async def acquire_locks(self, conn: SAConnection):
        self.import_mdl.release_queue()


def add_aiohttp_handlers(router: UrlDispatcher):
    class DelayImportsView(ImportsView):
        ServiceT = DelayImportService

    router.add_post(delay_imports_url, DelayImportsView)

    class NoLocksImportsView(ImportsView):
        ServiceT = NoLocksImportService

    router.add_post(no_locks_imports_url, NoLocksImportsView)

    class NoQueueImportsView(ImportsView):
        ServiceT = NoQueueImportService

    router.add_post(no_queue_imports_url, NoQueueImportsView)

    class DelayDeleteNodeView(DeleteNodeView):
        ServiceT = DelayNodeImportService

    router.add_delete(delay_delete_node_url, DelayDeleteNodeView)

    class NoLocksDeleteNodeView(DeleteNodeView):
        ServiceT = NoLocksNodeImportService

    router.add_delete(no_locks_delete_node_url, NoLocksDeleteNodeView)


def create_fastapi_router():
    router = APIRouter()

    @router.post(delay_imports_url)
    async def delay_imports(service: DelayImportService = service_depends(DelayImportService)):
        await service.execute_post_import()
        return Response()

    @router.post(no_locks_imports_url)
    async def no_locks_imports(service: NoLocksImportService = service_depends(NoLocksImportService)):
        await service.execute_post_import()
        return Response()

    @router.post(no_queue_imports_url)
    async def no_queue_imports(service: NoQueueImportService = service_depends(NoQueueImportService)):
        await service.execute_post_import()
        return Response()

    @router.delete(delay_delete_node_url)
    async def delay_delete_node(service: DelayNodeImportService = service_depends(DelayNodeImportService)):
        await service.execute_delete_node()
        return Response()

    @router.delete(no_locks_delete_node_url)
    async def no_locks_delete_node(service: NoLocksNodeImportService = service_depends(NoLocksNodeImportService)):
        await service.execute_delete_node()
        return Response()

    return router


@pytest.fixture
async def api_client(arguments, is_aiohttp):
    if is_aiohttp:
        app = create_aiohttp_app(arguments)
        add_aiohttp_handlers(app.router)

        server = TestServer(
            app,
            host=str(arguments.api_address),
            port=arguments.api_port
        )
        client = TestClient(server)

        try:
            await client.start_server()
            yield client
        finally:
            await client.close()
    else:
        app = create_fastapi_app(arguments)
        app.include_router(create_fastapi_router())

        async with AsyncClient(
                app=app,
                base_url="http://test"
        ) as client:
            try:
                await app.router.startup()
                yield client
            finally:
                await app.router.shutdown()


@pytest.mark.parametrize(
    'url, expected_status',
    [
        (delay_imports_url, HTTPStatus.OK),
        (no_locks_imports_url, HTTPStatus.BAD_REQUEST)
    ])
async def test_parent_exist(api_client, fake_cloud, sync_connection, url, expected_status):
    """
    without lock next import will not see parent folder from previous import.
    """
    n = 2
    parent_id = None
    for _ in range(n):
        fake_cloud.generate_import()
        folder = Folder(parent_id=parent_id)
        parent_id = folder.id
        fake_cloud.insert_item(folder)

    imports = [fake_cloud.get_import_dict(i) for i in range(-n, 0)]
    data_iter = iter(imports)

    coro1 = post_import(api_client, next(data_iter), path=url)
    coros = [post_import(api_client, data, path=url, expected_status=expected_status) for data in data_iter]

    await asyncio.gather(coro1, *coros)
    if expected_status == HTTPStatus.OK:
        compare_db_fc_state(sync_connection, fake_cloud)


@pytest.mark.parametrize(
    'url, expectation',
    [
        (delay_imports_url, nullcontext()),
        (no_locks_imports_url, pytest.raises(UniqueViolationError))
    ])
async def test_parents_updates(api_client, fake_cloud, sync_connection, url, expectation):
    """
    without lock second import will try to write same folder records in history.
    """
    fake_cloud.generate_import([[[]]])
    folder = fake_cloud[0, 0, 0]
    await post_import(api_client, fake_cloud.get_import_dict())

    n = 2
    for _ in range(n):
        fake_cloud.generate_import(1, parent_id=folder.id)

    imports = [fake_cloud.get_import_dict(i) for i in range(-n, 0)]
    corus = [post_import(api_client, data, path=url) for data in imports]
    with expectation:
        await asyncio.gather(*corus)
        compare_db_fc_state(sync_connection, fake_cloud)


@pytest.mark.parametrize(
    'url, expectation',
    [
        (delay_imports_url, nullcontext()),
        (no_locks_imports_url, pytest.raises(UniqueViolationError))
    ])
async def test_file_update(api_client, fake_cloud, sync_connection, url, expectation):
    """
    without lock handler will not see the file in db and try to insert it as a new file.
    """
    fake_cloud.generate_import(1)
    file = fake_cloud[0]
    c1 = post_import(api_client, fake_cloud.get_import_dict(), path=url)

    fake_cloud.generate_import()
    fake_cloud.update_item(file.id, size=10)
    c2 = post_import(api_client, fake_cloud.get_import_dict(), path=url)

    with expectation:
        await asyncio.gather(c1, c2)
        compare_db_fc_state(sync_connection, fake_cloud)


@pytest.mark.parametrize(
    'url, expectation',
    [
        (delay_imports_url, nullcontext()),
        (no_queue_imports_url, pytest.raises(AssertionError))
    ])
async def test_updates_order(api_client, fake_cloud, sync_connection, url, expectation):
    """
    First import a bunch of nested folders.
    Then two concurrent imports:
        - import file in bottom folder
        - then import file in top folder

    Without QueueWorker second concurrent transaction may
    lock top folder ID before first transaction.
    Simulation with sleep (see NoQueueImportService) doesn't look good.
    But this problem is not consistent without it. It becomes obvious in load testing.
    Or it can be recreated (without sleep) using this test with a huge number of nested folders.
    """

    fake_cloud.generate_import([])
    parent_id = fake_cloud[0].id
    top_folder = fake_cloud[0]

    n = 5
    for _ in range(n):
        folder = Folder(parent_id=parent_id)
        parent_id = folder.id
        fake_cloud.insert_item(folder)

    await post_import(api_client, fake_cloud.get_import_dict(), path=delay_imports_url)

    fake_cloud.generate_import(1, parent_id=parent_id)
    fake_cloud.generate_import(1, parent_id=top_folder.id)

    imports = [fake_cloud.get_import_dict(i) for i in range(-2, 0)]

    corus = [post_import(api_client, data, path=url) for data in imports]
    await asyncio.gather(*corus)

    with expectation:
        compare_db_fc_state(sync_connection, fake_cloud)


# noinspection PyTypeChecker
@pytest.mark.parametrize(
    'url, expectation',
    [
        (url_paths.IMPORTS, nullcontext()),
        (no_locks_imports_url, pytest.raises((AssertionError, UniqueViolationError)))
    ])
async def test_many_imports(api_client, sync_connection, url, expectation):
    fake_cloud = FakeCloudGen()
    n = 10
    for _ in range(n):
        fake_cloud.random_import(schemas_count=3, allow_random_count=False)
        fake_cloud.random_updates(allow_random_count=False)

    imports = [fake_cloud.get_import_dict(i) for i in range(-n, 0)]

    corus = [post_import(api_client, data, path=url) for data in imports]

    with expectation:
        await asyncio.gather(*corus)
        compare_db_fc_state(sync_connection, fake_cloud)


@pytest.mark.parametrize(
    'url, expected_status',
    [
        (url_paths.DELETE_NODE, HTTPStatus.OK),
        (no_locks_delete_node_url, HTTPStatus.BAD_REQUEST)
    ]
)
async def test_insert_and_del(api_client, sync_connection, fake_cloud, url, expected_status):
    fake_cloud.generate_import([[[]]])
    top_folder = fake_cloud[0]
    bot_folder = fake_cloud[0, 0, 0]

    await post_import(api_client, fake_cloud.get_import_dict())

    fake_cloud.generate_import(1, parent_id=bot_folder.id)
    coro1 = post_import(api_client, fake_cloud.get_import_dict(),
                        path=delay_imports_url, expected_status=expected_status)

    date = fake_cloud.del_item(top_folder.id)
    coro2 = del_node(api_client, top_folder.id, date, path=url)

    await asyncio.gather(coro1, coro2)
    if expected_status == HTTPStatus.OK:
        compare_db_fc_state(sync_connection, fake_cloud)


@pytest.mark.parametrize(
    'url, expectation',
    [
        (url_paths.DELETE_NODE, nullcontext()),
        (no_locks_delete_node_url, pytest.raises(AssertionError))
    ]
)
async def test_deletes_in_one_branch(api_client, sync_connection, fake_cloud, url, expectation):
    fake_cloud.generate_import([[[1], 1]])
    mid_folder = fake_cloud[0, 0]
    bot_folder = fake_cloud[0, 0, 0]
    await post_import(api_client, fake_cloud.get_import_dict())

    date = fake_cloud.del_item(bot_folder.id)
    coro1 = del_node(api_client, bot_folder.id, date, path=delay_delete_node_url)

    date = fake_cloud.del_item(mid_folder.id)
    coro2 = del_node(api_client, mid_folder.id, date, path=url)

    await asyncio.gather(coro1, coro2)
    with expectation:
        # wrong top folder history
        compare_db_fc_state(sync_connection, fake_cloud)
