import pytest
from alembic.command import upgrade
from sqlalchemy import create_engine

from disk.settings import Settings
from disk.utils.testing.test_client_factory import aiohttp_client_factory, fastapi_client_factory


@pytest.fixture
def migrated_postgres(alembic_config, postgres):
    upgrade(alembic_config, 'head')
    return postgres


@pytest.fixture
def arguments(aiomisc_unused_port, migrated_postgres: str):
    return Settings(
        api_address='127.0.0.1',
        api_port=aiomisc_unused_port,
        pg_dsn=migrated_postgres,
        log_level='debug'
    )


@pytest.fixture
async def api_client(is_aiohttp, arguments):
    factory = aiohttp_client_factory if is_aiohttp else fastapi_client_factory
    async with factory(arguments) as client:
        yield client


@pytest.fixture
def sync_connection(migrated_postgres: str):
    """
    sync connection to migrated db.
    """
    engine = create_engine(migrated_postgres)
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()
