from datetime import datetime, timezone, timedelta

from deepdiff import DeepDiff
from devtools import debug

from cloud.utils.testing import post_import, FakeCloud, compare_db_fc_state, get_node_history, get_updates


async def test_get_updates(api_client, migrated_postgres_sync_conn):

    fake_cloud = FakeCloud()

    # ################################################################### #
    fake_cloud.generate_import([1, [2]])
    await post_import(api_client, fake_cloud.get_import_dict())

    d1 = fake_cloud.get_node_copy('d1')
    d2 = fake_cloud.get_node_copy('d1/d1')
    f1 = fake_cloud.get_node_copy('d1/f1')
    f2 = fake_cloud.get_node_copy('d1/d1/f1')
    f3 = fake_cloud.get_node_copy('d1/d1/f2')
    # ################################################################### #

    # ################################################################### #
    date_end = datetime.now(timezone.utc) + timedelta(hours=1)
    expected_updates = fake_cloud.get_updates(date_end=date_end)
    received_updates = await get_updates(api_client, date_end)

    debug(date_end)
    ex_items = expected_updates['items']
    debug(len(set(i['id'] for i in ex_items)))
    debug(ex_items)
    debug(received_updates['items'])
    # compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    diff = DeepDiff(expected_updates, received_updates, ignore_order=True)
    assert diff == {}
