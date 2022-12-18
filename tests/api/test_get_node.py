from http import HTTPStatus

from pytest_cases import parametrize_with_cases
from deepdiff import DeepDiff

from cloud.utils.testing import get_node, File, direct_import_to_db, compare_db_fc_node_trees, Dataset
from tests import get_node_cases


@parametrize_with_cases('dataset', cases=get_node_cases)
async def test_with_static_data(api_client, sync_connection, dataset: Dataset):

    direct_import_to_db(sync_connection, dataset.import_dict)

    received_tree = await get_node(api_client, dataset.node_id)
    diff = DeepDiff(received_tree, dataset.expected_tree, ignore_order=True)
    assert diff == {}


async def test_with_generated_import(api_client, sync_connection, fake_cloud):

    fake_cloud.generate_import(2, [2, [], [2], [3, [1]]], [], [[]], [2])
    data = fake_cloud.get_import_dict()

    direct_import_to_db(sync_connection, data)

    await compare_db_fc_node_trees(api_client, fake_cloud)


async def test_non_existing_id(api_client, sync_connection, fake_cloud):
    fake_cloud.generate_import([1], 1)
    direct_import_to_db(sync_connection, fake_cloud.get_import_dict())
    await get_node(api_client, File().id, HTTPStatus.NOT_FOUND)
