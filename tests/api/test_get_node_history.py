from datetime import datetime, timezone

from deepdiff import DeepDiff
from devtools import debug

from cloud.utils.testing import post_import, FakeCloud, compare_db_fc_state, get_node_history, get_updates


async def test_get_node_history(api_client, migrated_postgres_sync_conn):

    fake_cloud = FakeCloud()

    # ################################################################### #
    fake_cloud.generate_import([1, [2, [2, [2]]]], 2, [])
    await post_import(api_client, fake_cloud.get_import_dict())

    f1 = fake_cloud.get_node_copy('d1/d1/d1/f1')
    f2 = fake_cloud.get_node_copy('d1/d1/d1/f2')
    f3 = fake_cloud.get_node_copy('f1')
    f4 = fake_cloud.get_node_copy('f2')

    d1 = fake_cloud.get_node_copy('d1')
    d2 = fake_cloud.get_node_copy('d2')
    d3 = fake_cloud.get_node_copy('d1/d1')
    d4 = fake_cloud.get_node_copy('d1/d1/d1')
    # ################################################################### #
    # import new file in d4 and update f2 in this dir
    fake_cloud.generate_import(1, parent_id=d4.id)
    fake_cloud.update_item(f2.id, size=50, url='la-la-lend')
    await post_import(api_client, fake_cloud.get_import_dict())

    date_start = datetime.now(timezone.utc)
    # ################################################################### #
    # update f4
    fake_cloud.generate_import()
    fake_cloud.update_item(f4.id, size=100, url='la-la-lend-2', parent_id=d1.id)
    await post_import(api_client, fake_cloud.get_import_dict())
    # ################################################################### #
    fake_cloud.generate_import()
    fake_cloud.update_item(d2.id, parent_id=d3.id)
    fake_cloud.update_item(f3.id, parent_id=d2.id)
    await post_import(api_client, fake_cloud.get_import_dict())
    date_end = datetime.now(timezone.utc)
    # ################################################################### #
    expected_node_history = fake_cloud.get_node_history(d1.id, date_start, date_end)
    received_node_history = await get_node_history(api_client, d1.id, date_start, date_end)

    # debug(date_start, date_end)
    # debug(d1.id)
    # debug(expected_node_history)
    # debug(received_node_history)
    # debug(fake_cloud.get_raw_db_imports_records())
    # compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    diff = DeepDiff(expected_node_history, received_node_history, ignore_order=True)
    assert diff == {}
    # ################################################################### #
    expected_updates = fake_cloud.get_updates(date_end=date_end)
    received_updates = await get_updates(api_client, date_end)

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    diff = DeepDiff(expected_updates, received_updates, ignore_order=True)
    assert diff == {}
