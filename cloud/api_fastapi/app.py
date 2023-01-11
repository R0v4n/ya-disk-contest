import json
from functools import partial

from fastapi import FastAPI

from .events import config_logging
from ..events import startup_pg, shutdown_pg
from .routers import router
from .errors import add_error_handlers
from cloud.settings import Settings, default_settings


def create_app(settings: Settings):
    app = FastAPI(docs_url='/')

    app.add_event_handler(
        'startup',
        partial(config_logging, settings=settings)
    )

    app.add_event_handler(
        'startup',
        partial(startup_pg, app, settings,
                lambda app, pg: setattr(app.state, 'pg', pg))
    )
    app.add_event_handler(
        'shutdown',
        partial(shutdown_pg, app, settings, lambda app: app.state.pg)
    )

    app.include_router(router)

    add_error_handlers(app)

    return app


# with open('/usr/share/python3/app/settings.json') as f:
#     settings = Settings(**json.load(f))
default_settings.pg_dsn = "postgresql://user:psw@db:5432/cloud"
app = create_app(default_settings)
