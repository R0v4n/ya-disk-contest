import logging
from pathlib import Path
from types import SimpleNamespace

from aiohttp.web_app import Application
from alembic.config import Config
from asyncpgsa import PG
from configargparse import Namespace
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
