from argparse import Namespace
from functools import partial

from aiohttp.web_app import Application

from cloud.utils.pg import pg_context
from .handlers import HANDLERS
from .middleware import error_middleware


# todo: add logging

def create_app(args: Namespace) -> Application:
    # todo: add middleware
    app = Application(middlewares=[error_middleware])
    app.cleanup_ctx.append(partial(pg_context, args=args))

    for controller in HANDLERS:
        app.router.add_route('*', controller.URL_PATH, controller)

    # todo: payload_registry? do i need it? check alvassin project
    return app
