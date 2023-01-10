from aiohttp.web_app import Application

from cloud.events import startup_pg, shutdown_pg
from cloud.settings import Settings


async def pg_context(app: Application, settings: Settings):
    await startup_pg(app, settings, lambda app, pg: app.__setitem__('pg', pg))
    try:
        yield
    finally:
        await shutdown_pg(app, settings, lambda app: app['pg'])
