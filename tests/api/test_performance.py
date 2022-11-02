from random import randint

from deepdiff import DeepDiff
from devtools import debug

from cloud.utils.testing import post_import, del_node, FakeCloudGen, compare_db_fc_state


async def test_api_in_action(api_client, sync_connection):
    fake_cloud = FakeCloudGen()

    fake_cloud.random_import(max_files_in_one_folder=2)
    # fake_cloud.generate_import([[]], [], [])
    import_data = fake_cloud.get_import_dict()

    await post_import(api_client, import_data)

    n = 10000
    check_count = 100
    check_step = n // check_count

    for step in range(n):
        fake_cloud.random_import(max_schema_count=4, max_files_in_one_folder=2)
        fake_cloud.random_updates(count=5)

        import_data = fake_cloud.get_import_dict()
        # last_correct_storage_tree = fake_cloud.get_tree()

        await post_import(api_client, import_data)

        for _ in range(randint(0, 2)):
            id_, date = fake_cloud.random_del()
            if id_:
                await del_node(api_client, id_, date)

        if step % check_step == 0 or step == n - 1:
            print(f'step={step}, node_count={len(fake_cloud.ids)}')
            # debug(fake_cloud.get_tree())
            try:
                compare_db_fc_state(sync_connection, fake_cloud)
            except AssertionError:
                debug(step)
                debug(import_data)
                debug(fake_cloud.get_tree())
                # debug(last_correct_storage_tree)
                raise

