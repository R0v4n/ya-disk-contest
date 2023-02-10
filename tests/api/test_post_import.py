from datetime import timedelta
from http import HTTPStatus
from itertools import accumulate

import pytest
from pytest_cases import parametrize, fixture, AUTO

from disk.db.schema import ItemType
from disk.utils.testing import post_import, FakeCloud, compare_db_fc_state, Folder, File, Dataset
from tests.post_import_cases import datasets

stairway_datasets = list(accumulate(datasets, lambda s, x: s + [x], initial=[]))


@pytest.mark.parametrize('datasets_stair', stairway_datasets)
async def test_with_static_data(
        fake_cloud,
        api_client,
        sync_connection,
        datasets_stair: list[Dataset]
):
    for d in datasets_stair:
        fake_cloud.load_import(d.import_dict)
        await post_import(api_client, d.import_dict)

    compare_db_fc_state(sync_connection, fake_cloud)


@pytest.fixture
async def filled_cloud(api_client, fake_cloud):
    fake_cloud.generate_import([1, [1, [1, [1]]]])
    await post_import(api_client, fake_cloud.get_import_dict())

    return fake_cloud


@fixture
def folder1(filled_cloud: FakeCloud):
    return filled_cloud[0]


@fixture
def folder2(filled_cloud: FakeCloud):
    return filled_cloud[0, 1]


@fixture
def folder3(filled_cloud: FakeCloud):
    return filled_cloud[0, 1, 1]


@fixture
def file1(filled_cloud: FakeCloud):
    return filled_cloud[0, 0]


@fixture
def new_empty_folder5_in_folder2(folder2):
    return [Folder(parent_id=folder2.id).import_dict]


@fixture
def child_and_parent_folders_swap(folder1, folder2, folder3):
    """
    update from d1/d2/d3/d4 to d1/d3/(d2, d4), d - is directory (folder).
    One of sophisticated cases that impossible for regular user to do in one update through standard app GUI
    """
    folder3.update(parent_id=folder1.id)
    folder2.update(parent_id=folder3.id)
    return [folder3.import_dict, folder2.import_dict]


@fixture
def file1_moved_in_folder3(folder3, file1):
    file1.update(parent_id=folder3.id)
    return [file1.import_dict]


@fixture
@parametrize(key=[0, (0, 0)])
def node(key, filled_cloud: FakeCloud):
    return filled_cloud[key]


@fixture
def nodes_updated_with_nonexistent_parent(node):
    node.update(parent_id='1')
    return node.import_dict


@fixture
def nodes_type_updated(node):
    if node.type == ItemType.FOLDER.value:
        return File(id=node.id).import_dict
    else:
        return Folder(id=node.id).import_dict


@fixture
def folder_with_size():
    d = Folder().import_dict
    d['size'] = 10
    return [d]


@fixture
@parametrize(
    items=[
        # two files with the same id
        [File(id='1').import_dict, File(id='1').import_dict],
        # file with size=0
        [File.construct(size=0).import_dict],
        # file with url=None
        [File.construct(url=None).import_dict],
        # folder with size
        folder_with_size,
        # folder with url
        [Folder.construct(url='foo').import_dict],
        # file with nonexistent parent
        [File(parent_id='1').import_dict],
        # folder with nonexistent parent
        [Folder(parent_id='1').import_dict],
        # update nodes with nonexistent parent
        nodes_updated_with_nonexistent_parent,
        # two folders with the same id
        [Folder(id='1').import_dict, Folder(id='1').import_dict],
        # file and folder with the same id
        [Folder(id='1').import_dict, File(id='1').import_dict],
        # attempt to update nodes type
        nodes_type_updated,
        # invalid import data format (items is dict)
        Folder().import_dict,
        # invalid import data format (extra field in item).
        [Folder().import_dict | {'foo': 'bar'}]
    ],
    idgen=AUTO
)
def bad_request_imports(items, filled_cloud: FakeCloud):
    date = str(filled_cloud.last_import_date + timedelta(seconds=1))

    import_dict = {
        'items': items,
        'updateDate': date
    }
    return import_dict


@fixture
def extra_field_import(filled_cloud: FakeCloud):
    date = str(filled_cloud.last_import_date + timedelta(seconds=1))

    import_dict = {
        'foo': 'bar',
        'items': [Folder().import_dict],
        'updateDate': date
    }
    return import_dict


@fixture
def reversed_items_import_data(filled_cloud: FakeCloud):
    p_id = filled_cloud[0].id
    filled_cloud.generate_import([[2, [[[]]]], 1, [1, []]], parent_id=p_id)
    import_data = filled_cloud.get_import_dict()
    import_data['items'].reverse()
    return import_data


@fixture
@parametrize(
    items=[
        # empty import
        [],
        # empty folder insertion should update parents
        # note: this behavior is my first assumption how api should work. It could be incorrect.
        #  Also, if delta size for any parent is equal to zero, it's considered updated anyway.
        new_empty_folder5_in_folder2,
        # file moved down in branch
        file1_moved_in_folder3,
        # update from d1/d2/d3/d4 to d1/d3/(d2, d4)
        child_and_parent_folders_swap
    ]
)
def ok_imports(items, filled_cloud: FakeCloud):
    date = str(filled_cloud.last_import_date + timedelta(seconds=1))

    import_dict = {
        'items': items,
        'updateDate': date
    }

    return import_dict


@pytest.mark.asyncio
@parametrize(
    'import_data, expected_status',
    [
        (ok_imports, HTTPStatus.OK),
        (bad_request_imports, HTTPStatus.BAD_REQUEST),
        (extra_field_import, HTTPStatus.BAD_REQUEST),
    ]
)
async def test_import_cases(
        import_data,
        expected_status,
        api_client,
        filled_cloud: FakeCloud,
        sync_connection,
):
    if expected_status == HTTPStatus.OK:
        filled_cloud.load_import(import_data)
    await post_import(api_client, import_data, expected_status=expected_status)

    compare_db_fc_state(sync_connection, filled_cloud)


async def test_reversed_items_order(
        filled_cloud: FakeCloud,
        reversed_items_import_data,
        api_client,
        sync_connection
):
    await post_import(api_client, reversed_items_import_data, expected_status=HTTPStatus.OK)
    compare_db_fc_state(sync_connection, filled_cloud)
