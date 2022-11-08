from datetime import timedelta
from http import HTTPStatus

import pytest
from deepdiff import DeepDiff

from cloud.utils.testing import post_import, FakeCloud, compare_db_fc_state, Folder, get_node, File
from tests.api.datasets import dataset_for_post_import


async def test_with_static_data(api_client, fake_cloud, sync_connection):
    dataset = dataset_for_post_import()

    for batch in dataset.import_dicts:
        fake_cloud.load_import(batch)
        await post_import(api_client, batch)

        compare_db_fc_state(sync_connection, fake_cloud)

    received_tree = await get_node(api_client, dataset.node_id)
    assert DeepDiff(received_tree, dataset.expected_tree, ignore_order=True) == {}


async def test_with_fake_cloud(api_client, fake_cloud, sync_connection):
    fake_cloud.generate_import([1, []])

    d1 = fake_cloud.get_node_copy('d1')
    d2 = fake_cloud.get_node_copy('d1/d1')
    f1 = fake_cloud.get_node_copy('d1/f1')

    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(sync_connection, fake_cloud)
    # ################################################################### #
    # empty folder insertion should update parents
    # warning: this behavior is my first assumption how api should work. Perhaps this is not correct.
    #  Also, if delta size for any parent is equal to zero, it's considered updated anyway.
    fake_cloud.generate_import([], parent_id=d2.id)

    d3 = fake_cloud.get_node_copy('d1/d1/d1')

    await post_import(api_client, fake_cloud.get_import_dict())
    compare_db_fc_state(sync_connection, fake_cloud)

    # ################################################################### #
    # file moved down in branch
    fake_cloud.generate_import()
    fake_cloud.update_item(f1.id, parent_id=d3.id, size=f1.size + 100)
    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(sync_connection, fake_cloud)
    # ################################################################### #


async def test_child_and_parent_folder_swap(api_client, fake_cloud, sync_connection):
    """
    update from d1/d2/d3/d4 to d1/d3/(d2, d4). Here d2 is just a name, not number or location.
    One of sophisticated cases that impossible for regular user to do in one update through standard app GUI
    """

    fake_cloud.generate_import([[1, [1, [1]]]])
    d1 = fake_cloud.get_node_copy('d1')
    d2 = fake_cloud.get_node_copy('d1/d1')
    d3 = fake_cloud.get_node_copy('d1/d1/d1')

    await post_import(api_client, fake_cloud.get_import_dict())
    # ################################################################### #

    fake_cloud.generate_import()
    fake_cloud.update_item(d3.id, parent_id=d1.id)
    fake_cloud.update_item(d2.id, parent_id=d3.id)

    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(sync_connection, fake_cloud)


@pytest.fixture
async def prepare(api_client, fake_cloud):
    fake_cloud.generate_import([1])
    await post_import(api_client, fake_cloud.get_import_dict())
    d1 = fake_cloud.get_node_copy('d1')
    f1 = fake_cloud.get_node_copy('d1/f1')
    return fake_cloud, (d1.id, f1.id)


@pytest.fixture
def filled_cloud(prepare: tuple):
    return prepare[0]


@pytest.fixture
def ids(prepare: tuple):
    return prepare[1]


# cases = [
#     # empty import
#     ([], HTTPStatus.OK),
#     ([File(id='1').import_dict, File(id='1').import_dict], HTTPStatus.BAD_REQUEST),
#     ([Folder(id='1').import_dict, File(id='1').import_dict], HTTPStatus.BAD_REQUEST),
#     ([Folder(id='1').import_dict, Folder(id='1').import_dict], HTTPStatus.BAD_REQUEST),
#     ([File(parent_id='1').import_dict], HTTPStatus.BAD_REQUEST),
#
# ]


@pytest.fixture(params=list(range(13)))
def case(request, filled_cloud: FakeCloud, ids):
    fake_cloud = filled_cloud
    folder_id, file_id = ids

    # ################################################################### #
    # empty import
    import_dict = {
        'items': [],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res = [(import_dict, HTTPStatus.OK)]
    # ################################################################### #
    # attempt to import two files with the same id
    import_dict = {
        'items': [File(id='1').import_dict, File(id='1').import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # file with nonexistent parent
    import_dict = {
        'items': [File(parent_id='1').import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # folder with nonexistent parent
    import_dict = {
        'items': [Folder(parent_id='1').import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # update folder with nonexistent parent
    import_dict = {
        'items': [Folder(id=folder_id, parent_id='1').import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # update file with nonexistent parent
    import_dict = {
        'items': [File(id=file_id, parent_id='1').import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # attempt to import two folders with the same id
    import_dict = {
        'items': [Folder(id='1').import_dict, Folder(id='1').import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # attempt to import file and folder with the same id
    import_dict = {
        'items': [Folder(id='1').import_dict, File(id='1').import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # attempt to update item type folder -> file
    import_dict = {
        'items': [File(id=folder_id).import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # attempt to update item type file -> folder
    import_dict = {
        'items': [Folder(id=file_id).import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # invalid import data format (extra field).
    import_dict = {
        'foo': 'bar',
        'items': [Folder().import_dict],
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # invalid import data format (items is dict).
    import_dict = {
        'items': Folder().import_dict,
        'updateDate': str(fake_cloud.last_import_date + timedelta(seconds=1))
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    # invalid import data format (date without tz. Does it mistake by the way?)
    date = (fake_cloud.last_import_date + timedelta(seconds=1)).replace(tzinfo=None)
    import_dict = {
        'items': [Folder().import_dict],
        'updateDate': str(date)
    }

    res.append((import_dict, HTTPStatus.BAD_REQUEST))
    # ################################################################### #
    return res[request.param]


async def test_import_cases(
        api_client,
        fake_cloud,
        sync_connection,
        case):

    import_data, expected_status = case
    if expected_status == HTTPStatus.OK:
        fake_cloud.load_import(import_data)

    await post_import(api_client, import_data, expected_status=expected_status)

    compare_db_fc_state(sync_connection, fake_cloud)
