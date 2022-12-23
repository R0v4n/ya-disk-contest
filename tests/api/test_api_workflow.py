from datetime import timedelta
from random import randint, shuffle, choice, uniform

from devtools import debug

from cloud.utils.testing import (
    post_import, del_node, get_node, get_node_history, get_updates,
    FakeCloudGen, compare_db_fc_state, compare
)


async def test(api_client, sync_connection):
    fake_cloud = FakeCloudGen()

    fake_cloud.random_import()
    import_data = fake_cloud.get_import_dict()
    first_import_date = fake_cloud.last_import_date

    await post_import(api_client, import_data)

    n = 50
    check_count = n
    check_period = n // check_count

    for step in range(1, n+1):
        fake_cloud.random_import(schemas_count=3)
        fake_cloud.random_updates(count=4)

        import_data = fake_cloud.get_import_dict()
        shuffle(import_data['items'])

        await post_import(api_client, import_data)

        for _ in range(randint(0, 3)):
            id_, date = fake_cloud.random_del()
            if id_:
                await del_node(api_client, id_, date)

        if step % check_period == 0:
            try:
                compare_db_fc_state(sync_connection, fake_cloud)

                # get node:
                folder_ids = fake_cloud.folder_ids
                if folder_ids:
                    node_id = choice(folder_ids)
                    received_tree = await get_node(api_client, node_id)
                    debug(received_tree)
                    compare(received_tree, fake_cloud.get_tree(node_id))

                # get node history:
                ids = fake_cloud.ids
                if ids:
                    node_id = choice(ids)
                    delta = fake_cloud.last_import_date - first_import_date

                    ds = first_import_date + delta * uniform(-1, 1)
                    de = ds + delta * uniform(0.1, 1.5)
                    received_history = await get_node_history(api_client, node_id, ds, de)
                    compare(received_history, fake_cloud.get_node_history(node_id, ds, de))

                # get updates:
                date = first_import_date + uniform(0, 1) * (fake_cloud.last_import_date - first_import_date)
                date += timedelta(hours=randint(0, 24))
                received_updates = await get_updates(api_client, date)
                compare(received_updates, fake_cloud.get_updates(date_end=date))

            except AssertionError:
                debug(step)
                debug(import_data)
                debug(fake_cloud.get_tree())
                raise
