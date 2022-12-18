"""Case functions for pytest-cases. Used for testing API methods and FakeCloud"""
from cloud.utils.testing import Dataset


def case_folder_tree():
    import_data = {
        'items': [
            {
                'id': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                'parentId': None,
                'type': 'FOLDER',
                'url': None,
                'size': None
            },
            {
                'id': '4eaefb10-a86e-4f0d-b805-0db428199c77',
                'parentId': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                'type': 'FILE',
                'url': '/attorney/our.txt',
                'size': 31,
            },
            {
                'id': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                'parentId': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                'type': 'FOLDER',
                'url': None,
                'size': None
            },
            {
                'id': '47b1719c-2adf-4cae-a380-e0af06c19d39',
                'parentId': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                'type': 'FILE',
                'url': '/kind/consumer.jpeg',
                'size': 50,
            },
            {
                'id': '302dcb05-927e-4eb8-926e-1376dc46d2ef',
                'parentId': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                'type': 'FILE',
                'url': '/respond/something.numbers',
                'size': 82,
            },
            {
                'id': 'fad9ab70-d2c8-4ba3-9d6d-d7f3603c14b0',
                'parentId': '8a4ef646-59aa-4d94-8e85-732695b7a131',
                'type': 'FOLDER',
                'url': None,
                'size': None
            },
            {
                'id': 'ee1cfdcd-1752-45db-b48a-26042ce874c7',
                'parentId': 'fad9ab70-d2c8-4ba3-9d6d-d7f3603c14b0',
                'type': 'FILE',
                'url': '/up/discussion.mov',
                'size': 122,
            },
        ],
        'updateDate': '2022-10-25 09:05:43.818731+00:00',
    }
    id_ = '35e28bb6-95a3-4521-a802-9d1e968b1a6a'
    expected_tree = {
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

    return Dataset(import_data, id_, expected_tree)


def case_empty_folder():
    import_data = {
        'items': [
            {
                'id': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
                'parentId': None,
                'type': 'FOLDER',
                'url': None,
                'size': None
            }
            ],
        'updateDate': '2022-10-25 09:05:43.818731+00:00',
    }
    id_ = '35e28bb6-95a3-4521-a802-9d1e968b1a6a'
    expected_tree = {
        'id': '35e28bb6-95a3-4521-a802-9d1e968b1a6a',
        'parentId': None,
        'type': 'FOLDER',
        'url': None,
        'size': 0,
        'date': '2022-10-25 09:05:43.818731+00:00',
        'children': [],
    }

    return Dataset(import_data, id_, expected_tree)


def case_file():
    import_data = {
        'items': [
            {
                'id': '4eaefb10-a86e-4f0d-b805-0db428199c77',
                'parentId': None,
                'type': 'FILE',
                'url': '/attorney/our.txt',
                'size': 31,
            }
        ],
        'updateDate': '2022-10-25 09:05:43.818731+00:00',
    }
    id_ = '4eaefb10-a86e-4f0d-b805-0db428199c77'
    expected_tree = {
                'id': '4eaefb10-a86e-4f0d-b805-0db428199c77',
                'parentId': None,
                'type': 'FILE',
                'url': '/attorney/our.txt',
                'size': 31,
                'date': '2022-10-25 09:05:43.818731+00:00',
                'children': None,
            }

    return Dataset(import_data, id_, expected_tree)


