from http import HTTPStatus

import pytest
from deepdiff import DeepDiff

from cloud.utils.testing import get_node, File, import_dataset, compare_db_fc_node_trees
from tests.conftest import datasets_for_get_node

datasets = datasets_for_get_node()


@pytest.mark.parametrize('dataset', datasets)
async def test_with_static_data(api_client, sync_connection, dataset):

    import_dataset(sync_connection, dataset.import_dicts[0])

    received_tree = await get_node(api_client, dataset.node_id)
    diff = DeepDiff(received_tree, dataset.expected_tree, ignore_order=True)
    assert diff == {}


async def test_with_generated_import(api_client, sync_connection, fake_cloud):

    fake_cloud.generate_import(2, [2, [], [2], [3, [1]]], [], [[]], [2])
    import_data = fake_cloud.get_import_dict()

    import_dataset(sync_connection, import_data)

    await compare_db_fc_node_trees(api_client, fake_cloud)


async def test_non_existing_id(api_client, sync_connection, fake_cloud):
    fake_cloud.generate_import([1], 1)
    import_dataset(sync_connection, fake_cloud.get_import_dict())
    await get_node(api_client, File().id, HTTPStatus.NOT_FOUND)
