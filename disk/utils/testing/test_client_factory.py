from contextlib import asynccontextmanager

from aiohttp.test_utils import TestServer, TestClient
from httpx import AsyncClient

from disk.api_aiohttp.app import create_app as create_aiohttp_app
from disk.api_fastapi.app import create_app as create_fastapi_app
from disk.settings import Settings


@asynccontextmanager
async def aiohttp_client_factory(settings: Settings):
    app = create_aiohttp_app(settings)
    server = TestServer(
        app,
        host=str(settings.api_address),
        port=settings.api_port
    )
    client = TestClient(server)

    try:
        await client.start_server()
        yield client
    finally:
        await client.close()


@asynccontextmanager
async def fastapi_client_factory(settings: Settings):
    app = create_fastapi_app(settings)
    async with AsyncClient(
            app=app,
            base_url="http://test"
    ) as client:
        try:
            await app.router.startup()
            yield client
        finally:
            await app.router.shutdown()
