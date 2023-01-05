from functools import partial

from fastapi import FastAPI

from .events import startup_pg, shutdown_pg
from .routers import router
from .errors import add_error_handlers
from cloud.settings import Settings


def create_app(settings: Settings):
    app = FastAPI(docs_url='/')

    app.add_event_handler(
        'startup',
        partial(startup_pg, app=app, settings=settings)
    )

    app.add_event_handler(
        'shutdown',
        partial(shutdown_pg, app)
    )

    app.include_router(router)

    add_error_handlers(app)

    return app
