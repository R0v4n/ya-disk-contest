from datetime import datetime
from enum import Enum
from http import HTTPStatus
from typing import Coroutine, Any, Iterable

from aiohttp.web_urldispatcher import DynamicResource

from cloud.resources import url_paths


class ResponseProxy:
    """Proxy class for aiohttp.ClientResponse and httpx.ClientResponse"""

    _sentinel = object()

    def __init__(self, response):
        self.response = response

    @classmethod
    def _check_attr(cls, attr: Any):
        if attr is cls._sentinel:
            raise TypeError('Unsupported response type')

    def _get_response_attr(self, names: str | Iterable[str]):
        if isinstance(names, str):
            names = names,
        attr = self._sentinel
        for name in names:
            attr = getattr(self.response, name, self._sentinel)
            if attr is not self._sentinel:
                break

        self._check_attr(attr)

        return attr

    @property
    def status(self):
        return self._get_response_attr(['status', 'status_code'])

    async def json(self):
        json_method = self._get_response_attr('json')

        data = json_method()
        if isinstance(data, Coroutine):
            return await data
        else:
            return data


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
    # todo: refactor url
    url = DynamicResource(path).url_for(**path_params)
    if query_params:
        url = url.update_query(**query_params)

    return str(url)


def expected_error_response(http_status_code: int):
    msg_dict = {
        HTTPStatus.NOT_FOUND: 'Item not found',
        HTTPStatus.BAD_REQUEST: 'Validation failed'
    }
    return {
        'code': http_status_code,
        'message': msg_dict.get(http_status_code, HTTPStatus(http_status_code).description)
    }


async def check_response(response: ResponseProxy, expected_status: int):

    assert response.status == expected_status

    if expected_status != HTTPStatus.OK:
        error = await response.json()
        assert error == expected_error_response(expected_status)


async def post_import(
        client,
        data: dict,
        expected_status: int | Enum = HTTPStatus.OK,
        url: str = url_paths.IMPORTS,
        **request_kwargs):

    res = await client.post(url, json=data, **request_kwargs)
    response = ResponseProxy(res)

    await check_response(response, expected_status)
    return response


async def get_node(
        client,
        node_id: str,
        expected_status: int | Enum = HTTPStatus.OK,
        url: str = url_paths.GET_NODE,
        **request_kwargs) -> list[dict] | None:

    res = await client.get(
        url_for(url, dict(node_id=node_id)),
        **request_kwargs
    )

    response = ResponseProxy(res)

    await check_response(response, expected_status)

    if response.status == HTTPStatus.OK:
        data = await response.json()
        return data


async def del_node(
        client,
        node_id: str,
        date: datetime | str,
        expected_status: int | Enum = HTTPStatus.OK,
        url: str = url_paths.DELETE_NODE,
        **request_kwargs):
    res = await client.delete(
        url_for(
            url,
            path_params=dict(node_id=node_id),
            query_params=dict(date=date)
        ),
        **request_kwargs
    )

    response = ResponseProxy(res)
    await check_response(response, expected_status)
    return response


async def get_updates(
        client,
        date: datetime | str,
        expected_status: int | Enum = HTTPStatus.OK,
        url: str = url_paths.GET_UPDATES,
        **request_kwargs):

    res = await client.get(
        url_for(
            url,
            query_params=dict(date=date)
        ),
        **request_kwargs
    )

    response = ResponseProxy(res)
    await check_response(response, expected_status)

    if response.status == HTTPStatus.OK:
        data = await response.json()
        return data


async def get_node_history(
        client,
        node_id: str,
        date_start: datetime | str,
        date_end: datetime | str,
        expected_status: int | Enum = HTTPStatus.OK,
        url: str = url_paths.GET_NODE_HISTORY,
        **request_kwargs):

    res = await client.get(
        url_for(
            url,
            path_params=dict(node_id=node_id),
            query_params=dict(dateStart=date_start, dateEnd=date_end)
        ),
        **request_kwargs
    )
    response = ResponseProxy(res)
    await check_response(response, expected_status)

    if response.status == HTTPStatus.OK:
        data = await response.json()
        return data
