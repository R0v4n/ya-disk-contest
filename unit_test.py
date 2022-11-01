# encoding=utf8

import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

from devtools import debug

from tests.api.datasets import EXPECTED_TREE, IMPORT_BATCHES

API_BASEURL = "http://localhost:8081"

ROOT_ID = "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1"

IMPORT2 = {
        'items': [
            {
                'id': 'eba2deaa-fdc6-468f-9d22-b061a2996d06',
                'parentId': None,
                'type': 'FOLDER',
                'url': None,
                'size': None,
            },
            {
                'id': '0ac48492-ef89-4662-b7ca-b3aef9d021dd',
                'parentId': 'eba2deaa-fdc6-468f-9d22-b061a2996d06',
                'type': 'FILE',
                'url': '/company/short.html',
                'size': 9,
            },
            {
                'id': '0da9559a-e7f8-41ea-a645-b5e2b15fb30b',
                'parentId': 'eba2deaa-fdc6-468f-9d22-b061a2996d06',
                'type': 'FILE',
                'url': '/land/military.jpeg',
                'size': 3,
            },
            {
                'id': '9af90060-3f8c-46c0-afaa-7c1bd9435bbe',
                'parentId': None,
                'type': 'FOLDER',
                'url': None,
                'size': None,
            },
            {
                'id': '93dd24b6-5b52-486c-aba1-89bd2f0799a9',
                'parentId': '9af90060-3f8c-46c0-afaa-7c1bd9435bbe',
                'type': 'FILE',
                'url': '/property/foreign.tiff',
                'size': 7,
            },
            {
                'id': 'fbb87982-2a76-4c8e-946c-e5a90c649f95',
                'parentId': None,
                'type': 'FILE',
                'url': '/together/baby.wav',
                'size': 2,
            },
            {
                'id': '61c71f93-a6c8-4832-8d3d-b2eaa760adb9',
                'parentId': '9af90060-3f8c-46c0-afaa-7c1bd9435bbe',
                'type': 'FOLDER',
                'url': None,
                'size': None,
            },
            {
                'id': '78f2f0ac-c5bb-40ef-9076-a31792b8e661',
                'parentId': '61c71f93-a6c8-4832-8d3d-b2eaa760adb9',
                'type': 'FILE',
                'url': '/power/top.txt',
                'size': 9,
            },
            {
                'id': '6d80bce7-619a-4cbb-b459-db2cb6bdbdc3',
                'parentId': '61c71f93-a6c8-4832-8d3d-b2eaa760adb9',
                'type': 'FILE',
                'url': '/although/clearly.webm',
                'size': 6,
            },
            {
                'id': 'cbb671de-c7dd-41c7-9df8-f5129234a778',
                'parentId': '61c71f93-a6c8-4832-8d3d-b2eaa760adb9',
                'type': 'FOLDER',
                'url': None,
                'size': None,
            },
            {
                'id': '30e01a7b-16b1-4cb4-aa7b-d065cb8b499a',
                'parentId': 'cbb671de-c7dd-41c7-9df8-f5129234a778',
                'type': 'FILE',
                'url': '/him/professional.odp',
                'size': 8,
            },
        ],
        'updateDate': '2022-10-29 06:00:22.915319+00:00',
    }

UPDATE_IMPORT = {
    "items": [
        {
            "type": "FILE",
            "url": "/file/url1",
            "id": "863e1a7a-1304-42ae-943b-179184c077e3",
            "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
            "size": 512
        },
        {
            "type": "FILE",
            "url": "/file/url2",
            "id": "b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4",
            "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "size": 256
        },
        {
            "type": "FOLDER",
            "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
            "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
        }

    ],
    "updateDate": "2022-02-03T19:00:00Z"
}


def request(path, method="GET", data=None, json_response=False):
    try:
        params = {
            "url": f"{API_BASEURL}{path}",
            "method": method,
            "headers": {},
        }
        print(params['url'])
        if data:
            params["data"] = json.dumps(
                data, ensure_ascii=False).encode("utf-8")
            params["headers"]["Content-Length"] = len(params["data"])
            params["headers"]["Content-Type"] = "application/json"

        req = urllib.request.Request(**params)

        with urllib.request.urlopen(req) as res:
            res_data = res.read().decode("utf-8")
            if json_response:
                res_data = json.loads(res_data)
            return (res.getcode(), res_data)
    except urllib.error.HTTPError as e:
        return (e.getcode(), None)


def deep_sort_children(node):
    if node.get("children"):
        node["children"].sort(key=lambda x: x["id"])

        for child in node["children"]:
            deep_sort_children(child)


def print_diff(expected, response):
    with open("expected.json", "w") as f:
        json.dump(expected, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")

    with open("response.json", "w") as f:
        json.dump(response, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")

    subprocess.run(["git", "--no-pager", "diff", "--no-index",
                    "expected.json", "response.json"])


def test_import(import_batches=None):
    import_batches = import_batches or [IMPORT2]
    for index, batch in enumerate(import_batches):
        print(f"Importing batch {index}")
        status, _ = request("/imports", method="POST", data=batch)

        assert status in (200, 201), f"Expected HTTP status code 200 or 201, got {status}"

    print("Test import passed.")


def test_nodes():
    status, response = request(f"/nodes/{ROOT_ID}", json_response=True)
    # print(json.dumps(response, indent=2, ensure_ascii=False))

    assert status == 200, f"Expected HTTP status code 200, got {status}"

    deep_sort_children(response)
    deep_sort_children(EXPECTED_TREE)
    if response != EXPECTED_TREE:
        print_diff(EXPECTED_TREE, response)
        print("Response tree doesn't match expected tree.")
        sys.exit(1)

    print("Test nodes passed.")


def test_updates(date=None):
    date = date or "2022-02-04T00:00:00Z"
    params = urllib.parse.urlencode({
        "date": date
    })
    status, response = request(f"/updates?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"
    debug(response)
    print("Test updates passed.")


def test_history(node_id=None, ds=None, de=None):
    node_id = node_id or ROOT_ID
    ds = ds or "2022-02-01T00:00:00Z"
    de = de or "2022-02-04T00:00:00Z"
    params = urllib.parse.urlencode({
        "dateStart": ds,
        "dateEnd": de
    })
    status, response = request(
        f"/node/{node_id}/history?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"
    debug(response)
    print("Test stats passed.")


def test_delete(node_id=None):
    node_id = node_id or ROOT_ID
    params = urllib.parse.urlencode({
        "date": "2022-02-04T00:00:00Z"
    })

    status, _ = request(f"/delete/{node_id}?{params}", method="DELETE")
    assert status == 200, f"Expected HTTP status code 200, got {status}"

    status, _ = request(f"/nodes/{node_id}", json_response=True)
    assert status == 404, f"Expected HTTP status code 404, got {status}"

    print("Test delete passed.")


def test_all():
    test_import()
    test_nodes()
    test_updates()
    test_history()
    test_delete()


def main():
    global API_BASEURL
    test_name = None

    for arg in sys.argv[1:]:
        if re.match(r"^https?://", arg):
            API_BASEURL = arg
        elif test_name is None:
            test_name = arg

    if API_BASEURL.endswith('/'):
        API_BASEURL = API_BASEURL[:-1]

    print(f"Testing API on {API_BASEURL}")

    if test_name is None:
        test_all()
    else:
        test_func = globals().get(f"test_{test_name}")
        if not test_func:
            print(f"Unknown test: {test_name}")
            sys.exit(1)
        test_func()


if __name__ == "__main__":
    main()
