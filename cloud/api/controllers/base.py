from asyncpgsa import PG
from aiohttp_pydantic import PydanticView


class BaseView(PydanticView):
    URL_PATH: str

    @property
    def pg(self) -> PG:
        return self.request.app['pg']
