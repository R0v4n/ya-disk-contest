import pytest
from alembic.command import upgrade
from sqlalchemy import create_engine

from cloud.api.app import create_app
from cloud.api.settings import Settings


@pytest.fixture
async def migrated_postgres(alembic_config, postgres):
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
async def api_client(aiohttp_client, arguments):
    app = create_app(arguments)
    # noinspection PyArgumentList
    client = await aiohttp_client(
        app,
        server_kwargs={
            'host': str(arguments.api_address),
            'port': arguments.api_port
        }
    )
    try:
        yield client
    finally:
        await client.close()


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
