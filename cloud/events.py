import logging
from typing import Callable

from asyncpgsa import PG
from yarl import URL

from .settings import Settings

logger = logging.getLogger(__name__)


async def startup_pg(app, settings: Settings, set_pg_attr: Callable):
    db_info = URL(settings.pg_dsn).with_password('***')
    logger.info('Connecting to database: %s', db_info)

    pg = PG()
    await pg.init(
        str(settings.pg_dsn),
        min_size=settings.pg_pool_min_size,
        max_size=settings.pg_pool_max_size
    )
    await pg.fetchval('SELECT 1')
    set_pg_attr(app, pg)
    logger.info('Connected to database %s', db_info)


async def shutdown_pg(app, settings: Settings, get_pg_attr: Callable):
    db_info = URL(settings.pg_dsn).with_password('***')
    logger.info('Disconnecting from database %s', db_info)
    await get_pg_attr(app).pool.close()
    logger.info('Disconnected from database %s', db_info)
