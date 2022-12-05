from random import randint

from devtools import debug

from cloud.utils.testing import post_import, del_node, FakeCloudGen, compare_db_fc_state


# todo: as it turned out i have another bug in folder history. It was also hidden due to the compare bug
async def test(api_client, sync_connection):
    fake_cloud = FakeCloudGen()

    fake_cloud.random_import()
    import_data = fake_cloud.get_import_dict()

    await post_import(api_client, import_data)

    n = 100
    check_count = n
    check_step = n // check_count

    for step in range(1, n+1):
        fake_cloud.random_import(schemas_count=3)
        fake_cloud.random_updates(count=4)

        import_data = fake_cloud.get_import_dict()
        # last_correct_storage_tree = fake_cloud.get_tree()

        await post_import(api_client, import_data)

        for _ in range(randint(0, 3)):
            id_, date = fake_cloud.random_del()
            if id_:
                await del_node(api_client, id_, date)

        if step % check_step == 0:
            # print(f'step={step}, node_count={len(fake_cloud.ids)}')
            try:
                compare_db_fc_state(sync_connection, fake_cloud)
            except AssertionError:
                debug(step)
                debug(import_data)
                debug(fake_cloud.get_tree())
                # debug(last_correct_storage_tree)
                raise

