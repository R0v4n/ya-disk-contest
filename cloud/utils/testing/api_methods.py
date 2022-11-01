from datetime import datetime
from enum import Enum
from http import HTTPStatus

from aiohttp.test_utils import TestClient
from aiohttp.web_urldispatcher import DynamicResource

from cloud.api.controllers import ImportsView, NodeView, UpdatesView, NodeHistoryView, DeleteNodeView


def url_for(path: str, path_params: dict = None, query_params: dict = None) -> str:
    """
    Generates URL for the aiohttp dynamic route with parameters.
    """

    def cast_types(dict_: dict | None):
        if dict_ is None:
            return {}

        return {
            key: str(value)
            for key, value in dict_.items()
        }

    path_params = cast_types(path_params)
    query_params = cast_types(query_params)

    url = DynamicResource(path).url_for(**path_params)
    if query_params:
        url = url.update_query(**query_params)

    return str(url)


async def post_import(
        client: TestClient,
        data: dict,
        expected_status: int | Enum = HTTPStatus.OK,
        **request_kwargs):

    response = await client.post(ImportsView.URL_PATH, json=data, **request_kwargs)
    assert response.status == expected_status


async def get_node(
        client: TestClient,
        node_id: str,
        expected_status: int | Enum = HTTPStatus.OK,
        **request_kwargs) -> list[dict]:

    response = await client.get(
        url_for(NodeView.URL_PATH, dict(node_id=node_id)),
        **request_kwargs
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.OK:
        data = await response.json()
        return data


async def del_node(
        client: TestClient,
        node_id: str,
        date: datetime | str,
        expected_status: int | Enum = HTTPStatus.OK,
        **request_kwargs):

    response = await client.delete(
        url_for(
            DeleteNodeView.URL_PATH,
            path_params=dict(node_id=node_id),
            query_params=dict(date=date)
        ),
        **request_kwargs
    )
    assert response.status == expected_status


async def get_updates(
        client: TestClient,
        date: datetime | str,
        expected_status: int | Enum = HTTPStatus.OK,
        **request_kwargs):

    response = await client.get(
        url_for(
            UpdatesView.URL_PATH,
            query_params=dict(date=date)
        ),
        **request_kwargs
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.OK:
        data = await response.json()
        return data


async def get_node_history(
        client: TestClient,
        node_id: str,
        date_start: datetime | str,
        date_end: datetime | str,
        expected_status: int | Enum = HTTPStatus.OK,
        **request_kwargs):

    response = await client.get(
        url_for(
            NodeHistoryView.URL_PATH,
            path_params=dict(node_id=node_id),
            query_params=dict(dateStart=date_start, dateEnd=date_end)
        ),
        **request_kwargs
    )
    assert response.status == expected_status

    if response.status == HTTPStatus.OK:
        data = await response.json()
        return data
