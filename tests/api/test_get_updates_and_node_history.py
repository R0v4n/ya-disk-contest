from datetime import timedelta

import pytest

from cloud.utils.testing import post_import, FakeCloud, get_updates, get_node_history, compare


@pytest.fixture(scope='module')
def cloud_fixtures():
    fake_cloud = FakeCloud()
    fake_cloud.generate_import([1, [2]])
    f1 = fake_cloud[0, 0]
    d2 = fake_cloud[0, 1]
    ids = f1.id, d2.id

    date = fake_cloud.last_import_date
    fake_cloud.generate_import(date=date+timedelta(hours=1))

    fake_cloud.update_item(f1.id, parent_id=d2.id)
    return fake_cloud, ids


@pytest.fixture(scope='module')
def cloud(cloud_fixtures):
    return cloud_fixtures[0]


# hours delta (relative to second import datetime):
deltas = [
    # before anything was imported
    -2,
    # first import datetime
    -1,
    # between imports
    -0.5,
    # second import datetime
    0,
    # 24.5 hours after first import. only one updated file should be in response
    23.5,
    # only one updated file should be in response
    24,
    # response should be empty
    25
]


@pytest.fixture(params=deltas, ids=(lambda d: f'hours_range: [{d-24}; {d}]'))
def date(request, cloud):
    return cloud.last_import_date + timedelta(hours=request.param)


async def test_updates(api_client, cloud, date):

    # note: In current version get_updates returns only last version if there are several updates in 24h.
    #  This can be incorrect. But I can't figure out what exactly was required by the task
    #  Anyway, it's very easy to change behavior to receive all versions for each file for 24h

    await post_import(api_client, cloud.get_import_dict(-2))
    await post_import(api_client, cloud.get_import_dict(-1))

    expected_updates = cloud.get_updates(date_end=date)
    received_updates = await get_updates(api_client, date)
    print(received_updates)
    compare(received_updates, expected_updates)


# -1 is first import datetime, 0 is second
deltas = [
    # empty
    (-48, -24),
    # empty (right-open range)
    (-2, -1),
    # first import record
    (-1, 0),
    # two records
    (-1, 0.5),
    # second import record
    (0, 1),
    # empty
    (1, 24),
]


@pytest.fixture(params=deltas, ids=(lambda d: f'hours_range: [{d[0]}; {d[1]})'))
def date_range(request, cloud):
    return tuple(cloud.last_import_date + timedelta(hours=d) for d in request.param)


@pytest.fixture(params=[0, 1], ids=['file_id', 'folder_id'])
def id_(request, cloud_fixtures):
    return cloud_fixtures[1][request.param]


async def test_node_history(api_client, cloud, date_range, id_):

    date_start, date_end = date_range

    await post_import(api_client, cloud.get_import_dict(-2))
    await post_import(api_client, cloud.get_import_dict(-1))

    expected_history = cloud.get_node_history(id_, date_start, date_end)
    received_history = await get_node_history(api_client, id_, date_start, date_end)

    compare(received_history, expected_history)
