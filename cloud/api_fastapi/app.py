from functools import partial

from asyncpgsa import pg
from fastapi import FastAPI

from cloud.api_fastapi.routers import router
from cloud.settings import Settings


async def startup_pg(args: Settings):
    await pg.init(
        str(args.pg_dsn),
        min_size=args.pg_pool_min_size,
        max_size=args.pg_pool_max_size
    )
    await pg.fetchval('SELECT 1')
    print('db connected')


async def shutdown_pg():
    await pg.pool.close()
    print('db disconnected')


def create_app(args: Settings):
    app = FastAPI(docs_url='/')

    app.include_router(router)

    app.on_event('startup')(partial(startup_pg, args))
    app.on_event('shutdown')(shutdown_pg)

    return app
