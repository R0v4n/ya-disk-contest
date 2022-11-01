from devtools import debug

from cloud.utils.testing import post_import, FakeCloud, del_node, compare_db_fc_state


async def test_delete_node_generated(api_client, migrated_postgres_sync_conn):
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
    # del f1
    del_date = fake_cloud.del_item(f1.id)
    await del_node(api_client, f1.id, del_date)

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #
    # import new file in d4 and update f2 in this dir
    fake_cloud.generate_import(1, parent_id=d4.id)
    fake_cloud.update_item(f2.id, size=50, url='la-la-lend')
    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #
    # del d3
    del_date = fake_cloud.del_item(d3.id)
    await del_node(api_client, d3.id, del_date)

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #
    # del d2
    del_date = fake_cloud.del_item(d2.id)
    await del_node(api_client, d2.id, del_date)

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    # ################################################################### #
    # del f3
    del_date = fake_cloud.del_item(f3.id)
    await del_node(api_client, f3.id, del_date)

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    debug(fake_cloud.get_tree())

    # ################################################################### #
    # update f4
    fake_cloud.generate_import()
    fake_cloud.update_item(f4.id, size=100, url='la-la-lend-2', parent_id=d1.id)
    await post_import(api_client, fake_cloud.get_import_dict())

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    debug(fake_cloud.get_tree())

    # ################################################################### #
    # del f4
    del_date = fake_cloud.del_item(f4.id)
    await del_node(api_client, f4.id, del_date)

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)
    debug(fake_cloud.get_tree())
    # ################################################################### #


async def test_delete(api_client, migrated_postgres_sync_conn):
    """Change file parent folder, then del this folder. File should stay in history."""
    fake_cloud = FakeCloud()
    fake_cloud.generate_import([1])
    await post_import(api_client, fake_cloud.get_import_dict())

    f1 = fake_cloud.get_node_copy('d1/f1')
    d1 = fake_cloud.get_node_copy('d1')

    fake_cloud.generate_import()
    fake_cloud.update_item(f1.id, parent_id=None)
    await post_import(api_client, fake_cloud.get_import_dict())

    date = fake_cloud.del_item(d1.id)
    await del_node(api_client, d1.id, date)

    compare_db_fc_state(migrated_postgres_sync_conn, fake_cloud)

