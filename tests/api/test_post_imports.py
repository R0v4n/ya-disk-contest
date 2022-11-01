from devtools import debug

from cloud.utils.testing import post_import, FakeCloud, compare_db_fc_state
from tests.api.datasets import IMPORT_BATCHES


async def test_post_imports(api_client, migrated_postgres_sync_conn):
    fake_cloud = FakeCloud()
    for batch in IMPORT_BATCHES:
        fake_cloud.load_import(batch)
        await post_import(api_client, batch)

        compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)


async def test_post_imports_generated(api_client, migrated_postgres_sync_conn):
    fake_cloud = FakeCloud()

    # ################################################################### #
    fake_cloud.generate_import([1])
    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #
    dir1 = fake_cloud.get_node_copy('d1')
    file1 = fake_cloud.get_node_copy('d1/f1')

    fake_cloud.generate_import([], parent_id=dir1.id)
    dir2 = fake_cloud.get_node_copy('d1/d1')
    fake_cloud.update_item(file1.id, parent_id=dir2.id)
    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #

# async def test_post_imports_generated(api_client, migrated_postgres_sync_conn):
#     fake_cloud = FakeCloud()
#
#     fake_cloud.generate_import(1, [2], [])
#     await compare_post(api_client, migrated_postgres_sync_conn, fake_cloud)
#     await asyncio.sleep(0.001)
#
#     dir1 = fake_cloud.get_node_copy('d1')
#     dir2 = fake_cloud.get_node_copy('d2')
#     file1 = fake_cloud.get_node_copy('f1')
#     fake_cloud.generate_import(1, [1], parent_id=dir1.id)
#     fake_cloud.update_item(file1.id, parent_id=dir1.id)
#     await compare_post(api_client, migrated_postgres_sync_conn, fake_cloud)
#     await asyncio.sleep(0.001)
#
#     fake_cloud.generate_import()
#     fake_cloud.update_item(file1.id, size=100)
#     await compare_post(api_client, migrated_postgres_sync_conn, fake_cloud)
#     await asyncio.sleep(0.001)
#
#     fake_cloud.generate_import()
#     fake_cloud.update_item(dir1.id, parent_id=dir2.id)
#     await compare_post(api_client, migrated_postgres_sync_conn, fake_cloud)
#     await asyncio.sleep(0.001)
#
#     fake_cloud.generate_import(3, parent_id=dir1.id)
#     await compare_post(api_client, migrated_postgres_sync_conn, fake_cloud)
#     await asyncio.sleep(0.001)


async def test_post_imports_case1(api_client, migrated_postgres_sync_conn):
    fake_cloud = FakeCloud()

    # ################################################################### #
    fake_cloud.generate_import([[1, [1, [1]]]])
    d1 = fake_cloud.get_node_copy('d1')
    d2 = fake_cloud.get_node_copy('d1/d1')
    d3 = fake_cloud.get_node_copy('d1/d1/d1')
    f3 = fake_cloud.get_node_copy('d1/d1/d1/f1')
    f2 = fake_cloud.get_node_copy('d1/d1/f1')

    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #
    debug(fake_cloud.get_tree())

    fake_cloud.generate_import()
    # fake_cloud.update_item(d3.id, parent_id=d1.id)
    # fake_cloud.update_item(d2.id, parent_id=d3.id)
    fake_cloud.update_item(f3.id, parent_id=d1.id)
    fake_cloud.update_item(f2.id, parent_id=d3.id)
    await post_import(api_client, fake_cloud.get_import_dict())
    debug(fake_cloud.get_tree())
    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #
