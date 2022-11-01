import pytest
from alembic.command import upgrade
from sqlalchemy import create_engine

from cloud.api.__main__ import parser
from cloud.api.app import create_app


@pytest.fixture
async def migrated_postgres(alembic_config, postgres):
    upgrade(alembic_config, 'head')
    return postgres


@pytest.fixture
def arguments(aiomisc_unused_port, migrated_postgres):
    return parser.parse_args(
        [
            # todo: add logging
            # '--log-level=debug',
            '--api-address=127.0.0.1',
            f'--api-port={aiomisc_unused_port}',
            f'--pg-url={migrated_postgres}'
        ]
    )


@pytest.fixture
async def api_client(aiohttp_client, arguments):
    app = create_app(arguments)
    client = await aiohttp_client(app,
                                  server_kwargs={
                                      'host': arguments.api_address,
                                      'port': arguments.api_port
                                  }
                                  )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
def migrated_postgres_sync_conn(migrated_postgres):
    """
    Синхронное соединение со смигрированной БД.
    """
    engine = create_engine(migrated_postgres)
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()


@pytest.fixture
def generated_datasets():
    pass
