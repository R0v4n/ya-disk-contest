from http import HTTPStatus

import pytest
from pytest_cases import parametrize_with_cases

from cloud.utils.testing import get_node, File, direct_import_to_db, Dataset, compare
from tests import get_node_cases


@parametrize_with_cases('dataset', cases=get_node_cases)
async def test_with_static_data(api_client, sync_connection, dataset: Dataset):
    direct_import_to_db(sync_connection, dataset.import_dict)

    received_tree = await get_node(api_client, dataset.node_id)
    compare(received_tree, dataset.expected_tree)


@pytest.mark.parametrize(
    'schema',
    [
        1,
        [],
        [1],
        [3],
        [[]],
        [[], [], []],
        [10, []],
        [[[2, [1], [3], [1, [1]]], 2], [[]], 10],
    ],
    ids=lambda x: str(x)
)
async def test_with_generated_import(schema, api_client, fake_cloud, sync_connection):
    """Folder sizes aren't calculated in this test."""
    fake_cloud.generate_import(schema)
    direct_import_to_db(sync_connection, fake_cloud.get_import_dict())
    node_id = fake_cloud[0].id

    received_tree = await get_node(api_client, node_id)

    compare(received_tree, fake_cloud.get_tree(node_id, nullify_folder_sizes=True))


async def test_non_existing_id(api_client, sync_connection, fake_cloud):
    fake_cloud.generate_import([1], 1)
    direct_import_to_db(sync_connection, fake_cloud.get_import_dict())
    await get_node(api_client, File().id, HTTPStatus.NOT_FOUND)
