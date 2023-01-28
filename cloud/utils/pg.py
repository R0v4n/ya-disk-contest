import logging
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from argparse import Namespace
from typing import Callable

from alembic.config import Config
from asyncpgsa import PG
from yarl import URL

from cloud.settings import Settings

CLOUD_PATH = Path(__file__).parent.parent.resolve()

logger = logging.getLogger(__name__)


def make_alembic_config(cmd_opts: SimpleNamespace | Namespace, base_path: str | Path = CLOUD_PATH) -> Config:
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


@asynccontextmanager
async def advisory_lock(conn, i: int):
    try:
        await conn.execute('SELECT pg_advisory_lock($1)', i)
        yield
    finally:
        await conn.execute('SELECT pg_advisory_unlock($1)', i)


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
