import pytest
from deepdiff import DeepDiff

from cloud.utils.testing import get_node, FakeCloud, import_dataset, compare_db_fc_node_trees

datasets = [
    (
        {
            'items': [
                {
                    'id': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                    'parent_id': None,
                    'type': 'FOLDER',
                },
                {
                    'id': '4eaefb10-a86e-4f0d-b805-0db428199c77',
                    'parent_id': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                    'type': 'FILE',
                    'url': '/attorney/our.txt',
                    'size': 31,
                },
                {
                    'id': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                    'parent_id': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                    'type': 'FOLDER',
                },
                {
                    'id': '47b1719c-2adf-4cae-a380-e0af06c19d39',
                    'parent_id': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                    'type': 'FILE',
                    'url': '/kind/consumer.jpeg',
                    'size': 50,
                },
                {
                    'id': '302dcb05-927e-4eb8-926e-1376dc46d2ef',
                    'parent_id': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                    'type': 'FILE',
                    'url': '/respond/something.numbers',
                    'size': 82,
                },
                {
                    'id': 'fad9ab70-d2c8-4ba3-9d6d-d7f3603c14b0',
                    'parent_id': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                    'type': 'FOLDER',
                },
                {
                    'id': 'ee1cfdcd-1752-45db-b48a-26042ce874c7',
                    'parent_id': 'fad9ab70-d2c8-4ba3-9d6d-d7f3603c14b0',
                    'type': 'FILE',
                    'url': '/up/discussion.mov',
                    'size': 122,
                },
            ],
            'updateDate': '2022-10-25 09:05:43.818731+00:00',
        },
        {
            'id': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
            'parentId': None,
            'type': 'FOLDER',
            'url': None,
            'size': 0,
            'date': '2022-10-25 09:05:43.818731+00:00',
            'children': [
                {
                    'id': '4eaefb10-a86e-4f0d-b805-0db428199c77',
                    'parentId': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                    'type': 'FILE',
                    'url': '/attorney/our.txt',
                    'size': 31,
                    'date': '2022-10-25 09:05:43.818731+00:00',
                    'children': None,
                },
                {
                    'id': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                    'parentId': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                    'type': 'FOLDER',
                    'url': None,
                    'size': 0,
                    'date': '2022-10-25 09:05:43.818731+00:00',
                    'children': [
                        {
                            'id': '47b1719c-2adf-4cae-a380-e0af06c19d39',
                            'parentId': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                            'type': 'FILE',
                            'url': '/kind/consumer.jpeg',
                            'size': 50,
                            'date': '2022-10-25 09:05:43.818731+00:00',
                            'children': None,
                        },
                        {
                            'id': '302dcb05-927e-4eb8-926e-1376dc46d2ef',
                            'parentId': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                            'type': 'FILE',
                            'url': '/respond/something.numbers',
                            'size': 82,
                            'date': '2022-10-25 09:05:43.818731+00:00',
                            'children': None,
                        },
                        {
                            'id': 'fad9ab70-d2c8-4ba3-9d6d-d7f3603c14b0',
                            'parentId': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                            'type': 'FOLDER',
                            'url': None,
                            'size': 0,
                            'date': '2022-10-25 09:05:43.818731+00:00',
                            'children': [
                                {
                                    'id': 'ee1cfdcd-1752-45db-b48a-26042ce874c7',
                                    'parentId': 'fad9ab70-d2c8-4ba3-9d6d-d7f3603c14b0',
                                    'type': 'FILE',
                                    'url': '/up/discussion.mov',
                                    'size': 122,
                                    'date': '2022-10-25 09:05:43.818731+00:00',
                                    'children': None,
                                },
                            ],
                        },
                    ],
                },
            ],
        }
    ),

]


# todo: add cases, use 2 params. refactor fake_cloud in fixture?
@pytest.mark.parametrize('dataset', datasets)
async def test_get_node(api_client, sync_connection, dataset):
    # fake_import = FakeImport(([5, [10, [2], [3, [5]]]],), ([[[2]]],))
    # dataset = fake_import.get_import_data()
    #
    # expected_tree = fake_import.node_trees[0]
    # node_id = fake_import.nodes[0]['id']
    import_data, expected_tree = dataset

    node_id = import_data['items'][0]['id']

    import_dataset(sync_connection, import_data)

    received_tree = await get_node(api_client, node_id)
    # todo: test DeepDiff
    diff = DeepDiff(received_tree, expected_tree, ignore_order=True)
    assert len(diff) == 0


async def test_get_node_dynamic(api_client, sync_connection):

    fake_cloud = FakeCloud()
    fake_cloud.generate_import(5, [10, [], [2], [3, [5]]], [], [[]], [2])
    import_data = fake_cloud.get_import_dict()

    import_dataset(sync_connection, import_data)

    await compare_db_fc_node_trees(api_client, fake_cloud)
