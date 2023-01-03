import logging
from functools import partial
from types import MappingProxyType
from typing import Mapping

from aiohttp.web_app import Application
from aiohttp import PAYLOAD_REGISTRY
from aiohttp_pydantic import oas

from cloud.utils.pg import pg_context
from .handlers import HANDLERS
from .middleware import error_middleware
from cloud.settings import Settings
from .payloads import JsonPayload

logger = logging.getLogger(__name__)


def create_app(args: Settings) -> Application:

    app = Application(middlewares=[error_middleware])
    oas.setup(app)
    app.cleanup_ctx.append(partial(pg_context, args=args))

    for handler in HANDLERS:
        logger.debug('Registering handler %r as %r', handler, handler.URL_PATH)
        app.router.add_route('*', handler.URL_PATH, handler)

    PAYLOAD_REGISTRY.register(JsonPayload, (Mapping, MappingProxyType))

    return app
