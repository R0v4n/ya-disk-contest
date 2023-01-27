import asyncio
import sys
from contextlib import nullcontext
from datetime import datetime, timezone, timedelta
from http import HTTPStatus

import pytest
from asyncpg import UniqueViolationError, DeadlockDetectedError
from asyncpgsa import PG
from fastapi import Depends
from httpx import AsyncClient
from rich import print
from starlette.responses import Response

from cloud import model
from cloud.api_fastapi.app import create_app
from cloud.api_fastapi.routers import get_pg
from cloud.model import ItemType
from cloud.resources import url_paths
from cloud.utils.testing import (
    post_import, get_node_history, del_node,
    compare_db_fc_state, Folder, File, get_node_records, FakeCloudGen
)

sys.setrecursionlimit(5000)

delay_imports_url = '/delay' + url_paths.IMPORTS
no_locks_imports_url = '/no-locks' + url_paths.IMPORTS
no_advisory_imports_url = '/no-advisory' + url_paths.IMPORTS

no_locks_delete_node_url = '/no-locks' + url_paths.DELETE_NODE

# todo: replace expectation with HTTPStatus somewhere
# todo: create outer router


class NoLocksNodeImportModel(model.NodeImportModel):
    async def acquire_locks(self):
        """do nothing"""


class NoAdvisoryLockImportModel(model.ImportModel):
    """
    Without these locks the first transaction may not have time to lock all records.
    See test_lock.
    """

    async def acquire_advisory_lock(self, i: int):
        """do nothing"""

    async def release_advisory_lock(self, i: int):
        """do nothing"""


class DelayImportModel(model.ImportModel):

    async def write_folders_history(self):
        await super().write_folders_history()
        print('waiting')
        await asyncio.sleep(1)


class NoLocksImportModel(DelayImportModel):
    async def acquire_ids_locks(self):
        """do nothing"""


async def patched_imports(mdl: DelayImportModel = Depends(), pg: PG = Depends(get_pg)):
    await mdl.init(pg)
    await mdl.execute_post_import()

    return Response()


async def no_locks_imports(mdl: NoLocksImportModel = Depends(), pg: PG = Depends(get_pg)):
    await mdl.init(pg)
    await mdl.execute_post_import()

    return Response()


async def no_locks_delete_node(mdl: NoLocksNodeImportModel = Depends(), pg: PG = Depends(get_pg)):
    await mdl.init(pg)
    await mdl.execute_delete_node()

    return Response()


async def no_advisory_imports(mdl: NoAdvisoryLockImportModel = Depends(), pg: PG = Depends(get_pg)):
    await mdl.init(pg)
    await mdl.execute_post_import()

    return Response()


@pytest.fixture
async def api_client(arguments):
    app = create_app(arguments)
    app.router.post(delay_imports_url, response_class=Response)(patched_imports)
    app.router.post(no_locks_imports_url, response_class=Response)(no_locks_imports)
    app.router.post(no_advisory_imports_url, response_class=Response)(no_advisory_imports)
    app.router.delete(no_locks_delete_node_url, response_class=Response)(no_locks_delete_node)

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
    'url, expectation',
    [
        (delay_imports_url, nullcontext()),
        (no_locks_imports_url, pytest.raises(AssertionError))
    ])
async def test_parent_exist(api_client, fake_cloud, sync_connection, url, expectation):
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

    corus = [post_import(api_client, data, path=url) for data in imports]
    with expectation:
        # this check runs only if no exceptions raised above
        await asyncio.gather(*corus)
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

    fake_cloud.generate_import()
    fake_cloud.update_item(file.id, size=10)

    imports = [fake_cloud.get_import_dict(i) for i in range(-2, 0)]

    corus = [post_import(api_client, data, path=url) for data in imports]
    with expectation:
        await asyncio.gather(*corus)
        compare_db_fc_state(sync_connection, fake_cloud)


@pytest.fixture
def bunch_count():
    return 6


@pytest.fixture
async def db_with_nested_folders(api_client, bunch_count):
    # note:
    #  There were some problems with recursion depth during my experiments.
    #  So in this test import data creating without fake_cloud (i don't want to fix recursion problems in fake cloud).

    date = datetime.now(timezone.utc)

    async def post_nested_folders(n: int, parent_id: str | None, dt: datetime):
        data = {'items': [], 'updateDate': dt.isoformat()}
        for _ in range(n):
            folder = Folder(parent_id=parent_id)
            parent_id = folder.id
            data['items'].append(folder.import_dict)

        await post_import(api_client, data)
        return data['items'][0], data['items'][-1]

    top_folder_dict, bottom_folder_dict = await post_nested_folders(200, None, date)

    last_parent_id = bottom_folder_dict['id']
    for _ in range(bunch_count - 1):
        date += timedelta(seconds=1)
        __, bottom_folder_dict = await post_nested_folders(200, last_parent_id, date)
        last_parent_id = bottom_folder_dict['id']

    return top_folder_dict, bottom_folder_dict, date


@pytest.mark.parametrize(
    'url, expectation',
    [
        (url_paths.IMPORTS, nullcontext()),
        (no_advisory_imports_url, pytest.raises(AssertionError))
    ])
async def test_advisory_lock(api_client, sync_connection, db_with_nested_folders, bunch_count,
                             url, expectation):
    """
    First import a bunch of nested folders.
    Then two concurrent imports:
        - first import file in bottom folder
        - second import file in top folder

    Without ImportModel.acquire_advisory_lock second concurrent transaction may
    lock top folder ID earlier than the first transaction.
    """

    top_folder_dict, bottom_folder_dict, date = db_with_nested_folders

    # ############# concurrent imports: first - file in bottom folder, second - file in top folder ############# #
    files_count = 1
    file_size = 10
    imports = [
        {
            'items': [File(parent_id=folder_dict['id'], size=file_size).import_dict
                      for _ in range(files_count)],
            'updateDate': (date + timedelta(seconds=i)).isoformat()
        } for i, folder_dict in enumerate([bottom_folder_dict, top_folder_dict], start=1)
    ]

    corus = [post_import(api_client, data, path=url) for data in imports]
    await asyncio.gather(*corus)

    # ############# get top folder size ############# #
    res = get_node_records(sync_connection, ItemType.FOLDER, ids=[top_folder_dict['id']])
    final_top_folder_size = res[0]['size']

    # ############# get top folder history ############# #
    date_end = datetime.fromisoformat(imports[-1]['updateDate'])
    res = await get_node_history(api_client, top_folder_dict['id'], date_end - timedelta(hours=1), date_end)
    items = res['items']
    items.sort(key=lambda x: datetime.fromisoformat(x['date']))

    expected_sizes = [0] * bunch_count + [file_size * files_count]

    with expectation:
        assert final_top_folder_size == 2 * file_size * files_count
        assert [item['size'] for item in items] == expected_sizes


# note: this test may fail sometimes with no_advisory_imports_url
#  (i.e. handler works correctly actually). I don't want to simulate transaction delay for this case.
@pytest.mark.parametrize(
    'url, expectation',
    [
        (url_paths.IMPORTS, nullcontext()),
        (no_advisory_imports_url, pytest.raises(AssertionError))
    ])
async def test_advisory_lock_with_fake_cloud(api_client, fake_cloud, sync_connection, url, expectation):
    """
    First import a bunch of nested folders.
    Then two concurrent imports:
        - first import file in bottom folder
        - second import file in top folder

    Without ImportModel.acquire_advisory_lock second concurrent transaction may
    lock top folder ID earlier than the first transaction.
    """

    fake_cloud.generate_import([])
    parent_id = fake_cloud[0].id
    top_folder = fake_cloud[0]

    n = 350
    for _ in range(n):
        folder = Folder(parent_id=parent_id)
        parent_id = folder.id
        fake_cloud.insert_item(folder)

    await post_import(api_client, fake_cloud.get_import_dict(), path=url)

    fake_cloud.generate_import(1, parent_id=parent_id)
    fake_cloud.generate_import(1, parent_id=top_folder.id)

    imports = [fake_cloud.get_import_dict(i) for i in range(-2, 0)]

    corus = [post_import(api_client, data, path=url) for data in imports]
    with expectation:
        await asyncio.gather(*corus)
        compare_db_fc_state(sync_connection, fake_cloud)


@pytest.mark.parametrize(
    'url, expectation',
    [
        (url_paths.DELETE_NODE, nullcontext()),
        (no_locks_delete_node_url, pytest.raises(DeadlockDetectedError))
    ])
async def test_delete(api_client, db_with_nested_folders, sync_connection, url, expectation):
    """
    First import a bunch of nested folders.
    Then two concurrent deletes:
        - first bottom folder
        - second top folder

    Without NodeImportModel.acquire_locks deadlock will be detected.
    """
    top_folder_dict, bottom_folder_dict, date = db_with_nested_folders

    coroutine1 = del_node(api_client, bottom_folder_dict['id'], date + timedelta(seconds=1), path=url)
    coroutine2 = del_node(api_client, top_folder_dict['id'], date + timedelta(seconds=2), path=url)

    with expectation:
        await asyncio.gather(coroutine1, coroutine2)
        assert get_node_records(sync_connection, ItemType.FOLDER) == []


@pytest.mark.parametrize(
    'url, status',
    [
        (url_paths.DELETE_NODE, HTTPStatus.OK),
        (no_locks_delete_node_url, HTTPStatus.NOT_FOUND)
    ])
async def test_insert_and_del(api_client, fake_cloud, sync_connection, url, status):
    """
    without lock delete node handler will not see item from previous import.
    """
    fake_cloud.generate_import(1)
    file = fake_cloud[0]

    c1 = post_import(api_client, fake_cloud.get_import_dict(), path=delay_imports_url)
    date = fake_cloud.del_item(file.id, fake_cloud.last_import_date + timedelta(seconds=1))

    c2 = del_node(api_client, file.id, date, expected_status=status, path=url)

    await asyncio.gather(c1, c2)
    if status == HTTPStatus.OK:
        compare_db_fc_state(sync_connection, fake_cloud)


async def test_insert(api_client, fake_cloud, sync_connection):
    """
    without lock delete node handler will not see item from previous import.
    """
    fake_cloud.generate_import(1)
    file = fake_cloud[0]
    c1 = post_import(api_client, fake_cloud.get_import_dict())

    fake_cloud.generate_import()
    fake_cloud.update_item(file.id, size=10)
    c2 = post_import(api_client, fake_cloud.get_import_dict())

    await asyncio.gather(c1, c2)
    compare_db_fc_state(sync_connection, fake_cloud)


async def test_many(api_client, sync_connection):
    fake_cloud = FakeCloudGen()
    n = 20
    for _ in range(n):
        fake_cloud.random_import(schemas_count=3, allow_random_count=False)
        fake_cloud.random_updates(allow_random_count=False)

    imports = [fake_cloud.get_import_dict(i) for i in range(-n, 0)]

    corus = [post_import(api_client, data) for data in imports]

    await asyncio.gather(*corus)
    compare_db_fc_state(sync_connection, fake_cloud)
    print(fake_cloud.get_tree())
