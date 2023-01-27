from functools import partial

from fastapi import FastAPI

from cloud.events import startup_pg, shutdown_pg
from cloud.settings import Settings
from cloud.utils.arguments_parse import clear_environ
from .errors import add_error_handlers
from .events import configure_logging, queue_worker_event
from .routers import router


def create_app(settings: Settings = None):
    settings = settings or Settings()
    clear_environ(lambda name: name.startswith(Settings.Config.env_prefix))

    app = FastAPI(docs_url='/')

    app.add_event_handler(
        'startup',
        partial(configure_logging, settings=settings)
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

    app.add_event_handler('startup', partial(queue_worker_event, settings.sleep))

    app.include_router(router)

    add_error_handlers(app)

    return app


app = create_app()
