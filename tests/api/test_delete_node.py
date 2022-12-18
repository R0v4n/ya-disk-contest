from datetime import datetime, timezone
from http import HTTPStatus

import pytest
from pytest_cases import parametrize_with_cases, parametrize

from cloud.utils.testing import post_import, FakeCloud, del_node, compare_db_fc_state, File


@pytest.fixture
async def filled_cloud(fake_cloud, api_client):
    fake_cloud.generate_import([[1, []], 1])
    await post_import(api_client, fake_cloud.get_import_dict())
    return fake_cloud


@pytest.mark.asyncio
async def case_file(filled_cloud):
    return filled_cloud[0, 0, 0].id


@pytest.mark.asyncio
async def case_file_with_history(filled_cloud: FakeCloud, api_client):
    file = filled_cloud[0, 0, 0]

    filled_cloud.generate_import()
    filled_cloud.update_item(file.id, size=100)
    await post_import(api_client, filled_cloud.get_import_dict())

    return file.id


@pytest.mark.asyncio
async def case_folder(filled_cloud: FakeCloud):
    return filled_cloud[0, 0].id


@pytest.mark.asyncio
async def case_folder_with_history(filled_cloud: FakeCloud, api_client):
    folder = filled_cloud[0, 0]

    filled_cloud.generate_import(1, parent_id=folder.id)
    await post_import(api_client, filled_cloud.get_import_dict())

    return folder.id


@pytest.mark.asyncio
async def case_folder_with_child_file_with_history(filled_cloud: FakeCloud, api_client):
    folder = filled_cloud[0, 0]
    child_file = filled_cloud[0, 0, 0]

    filled_cloud.generate_import()
    filled_cloud.update_item(child_file.id, url='lalaland')
    await post_import(api_client, filled_cloud.get_import_dict())

    return folder.id


@pytest.mark.asyncio
async def case_folder_with_child_folder_with_history(filled_cloud: FakeCloud, api_client):
    folder = filled_cloud[0, 0]
    child_folder = filled_cloud[0, 0, 1]

    filled_cloud.generate_import(1, parent_id=child_folder.id)
    await post_import(api_client, filled_cloud.get_import_dict())

    return folder.id


@pytest.mark.asyncio
async def case_del_file_old_parent(filled_cloud: FakeCloud, api_client):
    """
    Change file parent folder, then delete old parent folder. File records should stay in history.
    This was a bug actually (due to history table parent_id FK to folder table)
    """
    del_folder = filled_cloud[0, 0]
    top_folder = filled_cloud[0]
    child_file = filled_cloud[0, 0, 0]

    filled_cloud.generate_import()
    filled_cloud.update_item(child_file.id, parent_id=top_folder.id)
    await post_import(api_client, filled_cloud.get_import_dict())

    return del_folder.id


@pytest.mark.asyncio
async def case_del_folder_old_parent(filled_cloud: FakeCloud, api_client):
    """
    Change folder parent folder, then delete old parent folder. Folder records should stay in history.
    """
    del_folder = filled_cloud[0, 0]
    top_folder = filled_cloud[0]
    child_folder = filled_cloud[0, 0, 1]

    filled_cloud.generate_import()
    filled_cloud.update_item(child_folder.id, parent_id=top_folder.id)
    await post_import(api_client, filled_cloud.get_import_dict())

    return del_folder.id


@parametrize_with_cases('node_id', cases='.')
async def test(node_id, api_client, filled_cloud: FakeCloud, sync_connection):
    node_id = await node_id
    del_date = filled_cloud.del_item(node_id)
    await del_node(api_client, node_id, del_date)

    compare_db_fc_state(sync_connection, filled_cloud)


@pytest.fixture
def folder_id(filled_cloud: FakeCloud):
    return filled_cloud[0]


@pytest.mark.asyncio
@parametrize(
    ['node_id', 'date', 'expected_status'],
    [
        # non existing node id
        (File().id, datetime.now(timezone.utc), HTTPStatus.NOT_FOUND),
        # invalid date format (no tz)
        (folder_id, '2022-02-01 12:00:00', HTTPStatus.BAD_REQUEST)
    ]
)
async def test_bad_cases(api_client, sync_connection, node_id, date, expected_status):
    await del_node(api_client, node_id, date, expected_status)
