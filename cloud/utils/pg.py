from pathlib import Path
from types import SimpleNamespace

from aiohttp.web_app import Application
from alembic.config import Config
from asyncpgsa import PG
from configargparse import Namespace

DEFAULT_PG_URL = 'postgresql://rovan:hackme@localhost:5432/cloud'

CLOUD_PATH = Path(__file__).parent.parent.resolve()


async def pg_context(app: Application, args: Namespace):
    # todo: add logging

    app['pg'] = PG()
    await app['pg'].init(
        str(args.pg_url),
        min_size=args.pg_pool_min_size,
        max_size=args.pg_pool_max_size
    )

    await app['pg'].fetchval('SELECT 1')

    yield

    await app['pg'].pool.close()


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
    if cmd_opts.pg_url:
        config.set_main_option('sqlalchemy.url', cmd_opts.pg_url)

    return config
