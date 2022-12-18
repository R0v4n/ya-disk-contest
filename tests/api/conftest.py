from contextlib import asynccontextmanager, contextmanager

import pytest
from alembic.command import upgrade
from sqlalchemy import create_engine

from cloud.api.app import create_app
from cloud.api.settings import Settings


@pytest.fixture
def migrated_postgres(alembic_config, postgres):
    upgrade(alembic_config, 'head')
    return postgres


@pytest.fixture(scope='module')
def migrated_postgres_module(alembic_config_module, postgres_module):
    upgrade(alembic_config_module, 'head')
    return postgres_module


def _arguments(port, dsn: str):
    return Settings(
        api_address='127.0.0.1',
        api_port=port,
        pg_dsn=dsn,
        log_level='debug'
    )


@pytest.fixture
def arguments(aiomisc_unused_port, migrated_postgres: str):
    return _arguments(aiomisc_unused_port, migrated_postgres)


@pytest.fixture
def arguments_module(aiomisc_unused_port, migrated_postgres_module: str):
    """this fixture is function scope, but using pg dsn from module level fixture"""
    return _arguments(aiomisc_unused_port, migrated_postgres_module)


@asynccontextmanager
async def _api_client(client_factory, args):
    app = create_app(args)
    client = await client_factory(
        app,
        server_kwargs={
            'host': str(args.api_address),
            'port': args.api_port
        }
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def api_client(aiohttp_client, arguments):
    async with _api_client(aiohttp_client, arguments) as res:
        yield res


@pytest.fixture
async def api_client_module(aiohttp_client, arguments_module):
    """this fixture is function scope, but using pg dsn from module level fixture"""
    async with _api_client(aiohttp_client, arguments_module) as res:
        yield res


@contextmanager
def _sync_connection(dsn: str):
    """
    sync connection to migrated db.
    """
    engine = create_engine(dsn)
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()


@pytest.fixture
def sync_connection(migrated_postgres: str):
    """
    sync connection to migrated db.
    """
    with _sync_connection(migrated_postgres) as res:
        yield res


@pytest.fixture(scope='module')
def sync_connection_module(migrated_postgres_module: str):
    """
    sync connection to migrated db.
    """
    with _sync_connection(migrated_postgres_module) as res:
        yield res
