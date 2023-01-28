import logging
from functools import partial
from types import MappingProxyType
from typing import Mapping

from aiohttp import PAYLOAD_REGISTRY
from aiohttp.web_app import Application
from aiohttp_pydantic import oas

from cloud.settings import Settings
from cloud.utils.pg import startup_pg, shutdown_pg
from .handlers import HANDLERS
from .middleware import error_middleware
from .payloads import JsonPayload

logger = logging.getLogger(__name__)


async def pg_context(app: Application, settings: Settings):
    await startup_pg(app, settings, lambda ap, pg: ap.__setitem__('pg', pg))
    try:
        yield
    finally:
        await shutdown_pg(app, settings, lambda ap: ap['pg'])


def create_app(settings: Settings) -> Application:

    app = Application(middlewares=[error_middleware])
    oas.setup(app)
    app.cleanup_ctx.append(partial(pg_context, settings=settings))

    for handler in HANDLERS:
        logger.debug('Registering handler %r as %r', handler, handler.URL_PATH)
        app.router.add_route('*', handler.URL_PATH, handler)

    PAYLOAD_REGISTRY.register(JsonPayload, (Mapping, MappingProxyType))

    return app
