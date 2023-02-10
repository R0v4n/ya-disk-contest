from functools import partial

from fastapi import FastAPI

from disk.utils import startup_pg, shutdown_pg, clear_environ, QueueWorker
from disk.settings import Settings
from .errors import add_error_handlers
from .config_logging import configure_logging
from .routers import router


async def queue_worker_startup_event(app, settings):
    await QueueWorker.startup(app.state.pg, settings.sleep)


def create_app(settings: Settings = None):
    settings = settings or Settings()
    clear_environ(lambda name: name.startswith(Settings.Config.env_prefix))

    app = FastAPI()

    app.add_event_handler(
        'startup',
        partial(configure_logging, settings=settings)
    )

    app.add_event_handler(
        'startup',
        partial(startup_pg, app, settings,
                lambda ap, pg: setattr(ap.state, 'pg', pg))
    )
    app.add_event_handler(
        'shutdown',
        partial(shutdown_pg, app, settings, lambda ap: ap.state.pg)
    )

    app.add_event_handler('startup', partial(queue_worker_startup_event, app, settings))

    app.include_router(router)

    add_error_handlers(app)

    return app


app = create_app()
