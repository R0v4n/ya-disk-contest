from argparse import Namespace
from functools import partial

from aiohttp.web_app import Application

from cloud.utils.pg import pg_context
from .view import CONTROLLERS


# todo: add logging

def create_app(args: Namespace) -> Application:
    # todo: add middleware
    app = Application()
    app.cleanup_ctx.append(partial(pg_context, args=args))

    for controller in CONTROLLERS:
        app.router.add_route('*', controller.URL_PATH, controller)

    # todo: payload_registry? do i need it? check alvassin project
    return app
