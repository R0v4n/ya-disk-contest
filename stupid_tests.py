import urllib
import urllib.request
import urllib.error
import json
from pprint import pprint

from unit_test import request, test_import, ROOT_ID

API_BASEURL = "http://localhost:8080"

B2 = [
    {
        "items": [
            {
                "type": "FOLDER",
                "id": "d4",
                "parentId": "d1"
            }
        ],
        "updateDate": "2022-03-01T12:00:00Z"
    }]

IMPORT_BATCHES = [
    {
        "items": [
            {
                "type": "FOLDER",
                "id": "d1",
                "parentId": None
            }
        ],
        "updateDate": "2022-02-01T12:00:00Z"
    },
    {
        "items": [
            {
                "type": "FOLDER",
                "id": "d2",
                "parentId": "d1"
            },
            {
                "type": "FILE",
                "url": "/file/url1",
                "id": "f1",
                "parentId": "d2",
                "size": 128
            },
            {
                "type": "FILE",
                "url": "/file/url2",
                "id": "f2",
                "parentId": "d2",
                "size": 256
            }
        ],
        "updateDate": "2022-02-02T12:00:00Z"
    },
    {
        "items": [
            {
                "type": "FOLDER",
                "id": "d3",
                "parentId": "d1"
            },
            {
                "type": "FILE",
                "url": "/file/url3",
                "id": "f3",
                "parentId": "d3",
                "size": 512
            },
            {
                "type": "FILE",
                "url": "/file/url4",
                "id": "f4",
                "parentId": "d3",
                "size": 1024
            }
        ],
        "updateDate": "2022-02-03T12:00:00Z"
    },
    {
        "items": [
            {
                "type": "FILE",
                "url": "/file/url5",
                "id": "f5",
                "parentId": "d3",
                "size": 64
            }
        ],
        "updateDate": "2022-02-03T15:00:00Z"
    }
]


def test_nodes():
    status, response = request(f"/nodes/{ROOT_ID}", json_response=True)
    print(status)
    pprint(response)
    json.dump(response, open('scratch.json', 'w'), ensure_ascii=False, indent=4)
    # pprint(json.loads(response, indent=2, ensure_ascii=False))


# test_import(IMPORT_BATCHES)
# test_import(B2)
test_nodes()
