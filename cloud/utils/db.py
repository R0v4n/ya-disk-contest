from aiohttp.web_app import Application
from asyncpgsa import PG

DSN = 'postgresql://rovan:hackme@localhost:5432/cloud-db'


async def pg_context(app: Application):
    app['pg'] = PG()
    await app['pg'].init(DSN, min_size=10, max_size=10)

    # fixme: is it simple check?
    await app['pg'].fetchval('SELECT 1')

    yield

    await app['pg'].pool.close()

