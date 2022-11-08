from http import HTTPStatus

from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_response import json_response
from asyncpgsa import PG
from aiohttp_pydantic import PydanticView
from pydantic import ValidationError


class BasePydanticView(PydanticView):
    URL_PATH: str

    @property
    def pg(self) -> PG:
        return self.request.app['pg']

    async def on_validation_error(self,
                                  exception: ValidationError,
                                  context: str):

        raise HTTPBadRequest
