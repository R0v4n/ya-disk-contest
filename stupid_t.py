import json

from devtools import debug

from unit_test import request, test_import, ROOT_ID, test_updates

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


def test_nodes(node_id=None):
    node_id = node_id or ROOT_ID
    status, response = request(f"/nodes/{node_id}", json_response=True)
    print(status)
    debug(response)
    # json.dump(response, open('scratch.json', 'w'), ensure_ascii=False, indent=4)
    # pprint(json.loads(response, indent=2, ensure_ascii=False))


if __name__ == '__main__':

    test_import(None)
    # test_nodes('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1')

    # test_import([UPDATE_IMPORT])
    # test_nodes('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1')
    # test_nodes('b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4')

    # note: node in top folder f1:
    # test_delete('b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4')

    # note: deep node in all 3 folders:
    # test_delete('74b81fda-9cdc-4b63-8927-c978afed5cf4')

    # note: root folder:
    # test_delete('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1')

    # note: deepest folder f2:
    # test_delete('1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2')

    # test_nodes('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1')

    # test_updates("2022-02-03T00:00:00Z")
    # test_history()