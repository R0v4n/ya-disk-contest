import logging
from functools import partial
from types import AsyncGeneratorType, MappingProxyType
from typing import AsyncIterable, Mapping

from aiohttp import PAYLOAD_REGISTRY
from aiohttp.web_app import Application
from aiohttp_pydantic import oas

from cloud.utils.pg import pg_context
from .handlers import HANDLERS
from .middleware import error_middleware
from .payloads import JsonPayload, AsyncGenJsonListPayload, JsonNodeTreeListPayload
from .settings import Settings

logger = logging.getLogger(__name__)


def create_app(args: Settings) -> Application:

    app = Application(middlewares=[error_middleware])
    oas.setup(app)
    app.cleanup_ctx.append(partial(pg_context, args=args))

    for handler in HANDLERS:
        logger.debug('Registering handler %r as %r', handler, handler.URL_PATH)
        app.router.add_route('*', handler.URL_PATH, handler)

    PAYLOAD_REGISTRY.register(AsyncGenJsonListPayload,
                              (AsyncGeneratorType, AsyncIterable))
    PAYLOAD_REGISTRY.register(JsonPayload, (Mapping, MappingProxyType))
    # PAYLOAD_REGISTRY.register(JsonNodeTreeListPayload, list)
    return app
