from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp_pydantic import PydanticView as BasePydanticView
from asyncpgsa import PG
from pydantic import ValidationError


class PydanticView(BasePydanticView):
    URL_PATH: str

    @property
    def pg(self) -> PG:
        return self.request.app['pg']

    async def on_validation_error(self,
                                  exception: ValidationError,
                                  context: str):

        raise HTTPBadRequest
