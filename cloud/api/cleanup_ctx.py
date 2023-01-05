import logging

from aiohttp.web_app import Application
from asyncpgsa import PG
from yarl import URL

from cloud.settings import Settings

logger = logging.getLogger(__name__)


async def pg_context(app: Application, args: Settings):
    db_info = URL(args.pg_dsn).with_password('***')
    logger.info('Connecting to database: %s', db_info)

    app['pg'] = PG()
    await app['pg'].init(
        str(args.pg_dsn),
        min_size=args.pg_pool_min_size,
        max_size=args.pg_pool_max_size
    )

    await app['pg'].fetchval('SELECT 1')
    logger.info('Connected to database %s', db_info)

    try:
        yield
    finally:
        logger.info('Disconnecting from database %s', db_info)
        await app['pg'].pool.close()
        logger.info('Disconnected from database %s', db_info)
