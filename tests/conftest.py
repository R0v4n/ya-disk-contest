import os
import uuid
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from sqlalchemy_utils import create_database, drop_database
from yarl import URL

from cloud.api.settings import default_settings
from cloud.utils.pg import make_alembic_config
from cloud.utils.testing import FakeCloud


PG_DSN = os.getenv('CLOUD_PG_DSN', default_settings.pg_dsn)


@pytest.fixture
def postgres():
    tmp_name = f'{uuid.uuid4().hex}.pytest'
    tmp_url = str(URL(PG_DSN).with_path(tmp_name))
    create_database(tmp_url)

    try:
        yield tmp_url
    finally:
        drop_database(tmp_url)


@pytest.fixture
def alembic_config(postgres):

    cmd_options = SimpleNamespace(
        config='alembic.ini',
        name='alembic',
        pg_dsn=postgres,
        raiseerr=False,
        x=None
    )
    config = make_alembic_config(cmd_options)

    # config.set_section_option("logger_alembic", "level", "ERROR")
    return config


@pytest.fixture
def fake_cloud():
    return FakeCloud()


@dataclass
class Dataset:
    import_dicts: list[dict]
    node_id: str | None = None
    expected_tree: dict | None = None
    expected_history: list[dict] | None = None


@pytest.fixture()
def dataset_for_post_import():
    import_batches = [
        {
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
        },
        {
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
        },
        {
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
        },
        {
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
    ]
    expected_tree = {
        "type": "FOLDER",
        "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
        "size": 1984,
        "url": None,
        "parentId": None,
        "date": "2022-02-03 15:00:00+00:00",
        "children": [
            {
                "type": "FOLDER",
                "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "size": 1600,
                "url": None,
                "date": "2022-02-03 15:00:00+00:00",
                "children": [
                    {
                        "type": "FILE",
                        "url": "/file/url3",
                        "id": "98883e8f-0507-482f-bce2-2fb306cf6483",
                        "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                        "size": 512,
                        "date": "2022-02-03 12:00:00+00:00",
                        "children": None,
                    },
                    {
                        "type": "FILE",
                        "url": "/file/url4",
                        "id": "74b81fda-9cdc-4b63-8927-c978afed5cf4",
                        "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                        "size": 1024,
                        "date": "2022-02-03 12:00:00+00:00",
                        "children": None
                    },
                    {
                        "type": "FILE",
                        "url": "/file/url5",
                        "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                        "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                        "size": 64,
                        "date": "2022-02-03 15:00:00+00:00",
                        "children": None
                    }
                ]
            },
            {
                "type": "FOLDER",
                "id": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "size": 384,
                "url": None,
                "date": "2022-02-02 12:00:00+00:00",
                "children": [
                    {
                        "type": "FILE",
                        "url": "/file/url1",
                        "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                        "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                        "size": 128,
                        "date": "2022-02-02 12:00:00+00:00",
                        "children": None
                    },
                    {
                        "type": "FILE",
                        "url": "/file/url2",
                        "id": "b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4",
                        "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                        "size": 256,
                        "date": "2022-02-02 12:00:00+00:00",
                        "children": None
                    }
                ]
            },
        ]
    }
    expected_history = [
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

    return Dataset(
        import_batches,
        "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
        expected_tree,
        expected_history
    )


def datasets_for_get_node():
    datasets = []

    # ############################################################## #
    # folder tree
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

    tree_dataset = Dataset([import_data], id_, expected_tree)
    datasets.append(tree_dataset)
    # ############################################################## #
    # empty folder
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

    empty_folder_dataset = Dataset([import_data], id_, expected_tree)
    datasets.append(empty_folder_dataset)
    # ############################################################## #
    # just a file
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

    file_dataset = Dataset([import_data], id_, expected_tree)
    datasets.append(file_dataset)
    # ############################################################## #

    return datasets
