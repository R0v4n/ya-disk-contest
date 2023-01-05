from asyncpgsa import PG
from fastapi import FastAPI

from cloud.settings import Settings


async def startup_pg(app: FastAPI, settings: Settings):
    app.state.pg = PG()
    await app.state.pg.init(
        str(settings.pg_dsn),
        min_size=settings.pg_pool_min_size,
        max_size=settings.pg_pool_max_size
    )
    await app.state.pg.fetchval('SELECT 1')
    print('db connected')


async def shutdown_pg(app: FastAPI):
    await app.state.pg.pool.close()
    print('db disconnected')
