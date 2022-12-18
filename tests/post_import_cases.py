"""Case functions for pytest-cases. Used for testing API methods and FakeCloud"""
from cloud.utils.testing import Dataset


def case_folder1():
    data = {
        "items": [
            {
                "type": "FOLDER",
                "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "parentId": None,
                "url": None,
                "size": None
            }
        ],
        "updateDate": "2022-02-01 12:00:00+00:00"
    }

    return Dataset(import_dict=data, expected_history=[])


def case_folder2_with_two_files():
    data = {
        "items": [
            {
                "type": "FOLDER",
                "id": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "url": None,
                "size": None
            },
            {
                "type": "FILE",
                "url": "/file/url1",
                "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "size": 128
            },
            {
                "type": "FILE",
                "url": "/file/url2",
                "id": "b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "size": 256
            }
        ],
        "updateDate": "2022-02-02 12:00:00+00:00"
    }

    history = [
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-01 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 0,
        }
    ]

    return Dataset(import_dict=data, expected_history=history)


def case_folder3_with_two_files():
    data = {
        "items": [
            {
                "type": "FOLDER",
                "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "url": None,
                "size": None
            },
            {
                "type": "FILE",
                "url": "/file/url3",
                "id": "98883e8f-0507-482f-bce2-2fb306cf6483",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "size": 512
            },
            {
                "type": "FILE",
                "url": "/file/url4",
                "id": "74b81fda-9cdc-4b63-8927-c978afed5cf4",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "size": 1024
            }
        ],
        "updateDate": "2022-02-03 12:00:00+00:00"
    }

    history = [
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-01 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 0,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
    ]

    return Dataset(import_dict=data, expected_history=history)


def case_new_file_in_folder3():
    data = {
        "items": [
            {
                "type": "FILE",
                "url": "/file/url5",
                "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "size": 64
            }
        ],
        "updateDate": "2022-02-03 15:00:00+00:00"
    }

    history = [
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-01 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 0,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
        {
            'id': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1536,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1920,
        },
    ]

    return Dataset(import_dict=data, expected_history=history)


def case_update_file_in_folder2():
    data = {
        "items": [
            {
                "type": "FILE",
                "url": "/file/url1",
                "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "size": 200
            }
        ],
        "updateDate": "2022-06-03 18:00:00+07:00"
    }

    history = [
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-01 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 0,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
        {
            'id': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1536,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1920,
        },
        {
            'id': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-03 15:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1984,
        },
        {
            'id': '863e1a7a-1304-42ae-943b-179184c077e3',
            'parentId': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FILE',
            'url': '/file/url1',
            'size': 128,
        },
    ]

    return Dataset(import_dict=data, expected_history=history)


def case_move_file_from_folder2_to_folder3():
    data = {
        "items": [
            {
                "type": "FILE",
                "url": "/file/url1",
                "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "size": 200
            }
        ],
        "updateDate": "2022-06-03 18:00:00+06:00"
    }

    history = [
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-01 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 0,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
        {
            'id': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1536,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1920,
        },
        {
            'id': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-03 15:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1984,
        },
        {
            'id': '863e1a7a-1304-42ae-943b-179184c077e3',
            'parentId': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FILE',
            'url': '/file/url1',
            'size': 128,
        },
        {
            'id': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-06-03 18:00:00+07:00',
            'type': 'FOLDER',
            'url': None,
            'size': 456,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-06-03 18:00:00+07:00',
            'type': 'FOLDER',
            'url': None,
            'size': 2056,
        },
        {
            'id': '863e1a7a-1304-42ae-943b-179184c077e3',
            'parentId': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'date': '2022-06-03 18:00:00+07:00',
            'type': 'FILE',
            'url': '/file/url1',
            'size': 200,
        },
        {
            'id': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-03 15:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1600,
        },
    ]

    return Dataset(import_dict=data, expected_history=history)


def case_move_folder3_in_folder2():
    data = {
        "items": [
            {
                "type": "FOLDER",
                "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "url": None,
                "size": None
            }
        ],
        "updateDate": "2022-06-03 18:00:00+05:00"
    }

    history = [
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-01 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 0,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
        {
            'id': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1536,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-03 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1920,
        },
        {
            'id': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 384,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-02-03 15:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1984,
        },
        {
            'id': '863e1a7a-1304-42ae-943b-179184c077e3',
            'parentId': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'date': '2022-02-02 12:00:00+00:00',
            'type': 'FILE',
            'url': '/file/url1',
            'size': 128,
        },
        {
            'id': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-06-03 18:00:00+07:00',
            'type': 'FOLDER',
            'url': None,
            'size': 456,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-06-03 18:00:00+07:00',
            'type': 'FOLDER',
            'url': None,
            'size': 2056,
        },
        {
            'id': '863e1a7a-1304-42ae-943b-179184c077e3',
            'parentId': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'date': '2022-06-03 18:00:00+07:00',
            'type': 'FILE',
            'url': '/file/url1',
            'size': 200,
        },
        {
            'id': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-02-03 15:00:00+00:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1600,
        },
        {
            'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'parentId': None,
            'date': '2022-06-03 18:00:00+06:00',
            'type': 'FOLDER',
            'url': None,
            'size': 2056,
        },
        {
            'id': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-06-03 18:00:00+06:00',
            'type': 'FOLDER',
            'url': None,
            'size': 1800,
        },
        {
            'id': 'd515e43f-f3f6-4471-bb77-6b455017a2d2',
            'parentId': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
            'date': '2022-06-03 18:00:00+06:00',
            'type': 'FOLDER',
            'url': None,
            'size': 256,
        },
    ]

    return Dataset(import_dict=data, expected_history=history)
