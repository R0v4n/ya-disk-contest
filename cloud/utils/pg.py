import logging
from pathlib import Path
from types import SimpleNamespace
from typing import AsyncIterable, Callable

from aiohttp.web_app import Application
from alembic.config import Config
from asyncpgsa import PG
from asyncpgsa.transactionmanager import ConnectionTransactionContextManager
from configargparse import Namespace
from sqlalchemy.sql import Select
from yarl import URL

from cloud.api.settings import Settings


CLOUD_PATH = Path(__file__).parent.parent.resolve()


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


def make_alembic_config(cmd_opts: Namespace | SimpleNamespace, base_path: str | Path = CLOUD_PATH) -> Config:
    """
    Создает объект конфигурации alembic на основе аргументов командной строки,
    подменяет относительные пути на абсолютные.
    """
    # Подменяем путь до файла alembic.ini на абсолютный
    config_path = Path(cmd_opts.config)
    if not config_path.is_absolute():
        cmd_opts.config = base_path / config_path.name

    config = Config(file_=cmd_opts.config, ini_section=cmd_opts.name, cmd_opts=cmd_opts)

    # Подменяем путь до папки с alembic на абсолютный
    alembic_dir = Path(config.get_main_option('script_location'))
    if not alembic_dir.is_absolute():
        config.set_main_option('script_location', str(base_path / alembic_dir))
    if cmd_opts.pg_dsn:
        config.set_main_option('sqlalchemy.url', cmd_opts.pg_dsn)

    return config


class SelectQuery(AsyncIterable):
    """
    Используется чтобы отправлять данные из PostgreSQL клиенту сразу после
    получения, по частям, без буфферизации всех данных.
    """
    PREFETCH = 100

    __slots__ = (
        'query', 'transaction_ctx', 'prefetch', 'timeout', 'transform_record_func'
    )

    def __init__(self, query: Select,
                 transaction_ctx: ConnectionTransactionContextManager,
                 transform_record_func: Callable = None,
                 prefetch: int = None,
                 timeout: float = None):
        self.query = query
        self.transaction_ctx = transaction_ctx
        self.transform_record_func = transform_record_func
        self.prefetch = prefetch or self.PREFETCH
        self.timeout = timeout

    async def __aiter__(self):
        async with self.transaction_ctx as conn:
            cursor = conn.cursor(self.query, prefetch=self.prefetch,
                                 timeout=self.timeout)
            async for row in cursor:
                yield self.transform_record_func(row) if self.transform_record_func else row
