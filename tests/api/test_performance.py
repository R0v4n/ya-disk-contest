from datetime import datetime, timezone, timedelta

from deepdiff import DeepDiff
from devtools import debug

from cloud.utils.testing import post_import, del_node, FakeCloudGen, compare_db_fc_state


async def test_performance(api_client, migrated_postgres_sync_conn):

    fake_cloud = FakeCloudGen()

    fake_cloud.random_import()
    import_data = fake_cloud.get_import_dict()

    await post_import(api_client, import_data)

    for step in range(1000):
        fake_cloud.random_import()
        fake_cloud.random_updates(1)
        import_data = fake_cloud.get_import_dict()
        last_correct_storage_tree = fake_cloud.get_tree()

        await post_import(api_client, import_data)

        id_, date = fake_cloud.random_del()
        if id_:
            await del_node(api_client, id_, date)

        try:
            compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
        except AssertionError:
            debug(step)
            debug(date, id_)
            debug(import_data)
            debug(fake_cloud.get_tree())
            debug(last_correct_storage_tree)
            raise

    debug(len(fake_cloud.ids))
    debug(fake_cloud.get_tree())