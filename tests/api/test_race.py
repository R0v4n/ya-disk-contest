import asyncio

import pytest
from asyncpgsa import PG
from fastapi import Depends
from httpx import AsyncClient
from rich import print
from starlette.responses import Response

from cloud import model
from cloud.api_fastapi.routers import get_pg
from cloud.utils.testing import post_import, compare_db_fc_state, Folder, get_node
from cloud.api_fastapi.app import create_app


# class PatchedImportModel(model.ImportModel):
#     async def insert_import_with_model_id(self):


@pytest.fixture
async def race_client(arguments):
    app = create_app(arguments)
    router = app.router

    @router.post('race/imports', response_class=Response)
    async def imports(mdl: model.ImportModel = Depends(), pg: PG = Depends(get_pg)):
        await mdl.init(pg)
        await mdl.execute_post_import()

        return Response()

    async with AsyncClient(
            app=app,
            base_url="http://test"
    ) as client:
        try:
            await app.router.startup()
            yield client
        finally:
            await app.router.shutdown()


async def test(api_client, fake_cloud, sync_connection):
    """
    without imports table lock second import will not see parent folder from first import.
    """
    fake_cloud.generate_import([[1]])
    d1 = fake_cloud[0]
    d2 = fake_cloud[0, 0]
    await post_import(api_client, fake_cloud.get_import_dict())

    n = 2
    for _ in range(n):
        fake_cloud.generate_import(parent_id=d2.id)
        d2 = Folder(parent_id=d2.id)
        fake_cloud.insert_item(d2)
        fake_cloud.generate_import([2, [5, [[5], [50]]]], parent_id=d2.id, is_new=False)

    imports = [fake_cloud.get_import_dict(i) for i in range(-n, 0)]

    corus = [post_import(api_client, data) for data in imports]
    await asyncio.gather(*corus)
    # for c in corus:
    #     await c

    # tree = await get_node(api_client, d1.id)
    # with open('race.json', 'w') as f:
    #     json.dump(tree, f, indent=2)

    compare_db_fc_state(sync_connection, fake_cloud)


async def test_parents_updates(api_client, fake_cloud, sync_connection):
    """
    without imports table lock second import will try to write same folder records in history.
    """
    fake_cloud.generate_import([[[]]])
    d2 = fake_cloud[0, 0, 0]
    await post_import(api_client, fake_cloud.get_import_dict())

    n = 2
    for _ in range(n):
        fake_cloud.generate_import(100, parent_id=d2.id)

    imports = [fake_cloud.get_import_dict(i) for i in range(-n, 0)]
    corus = [post_import(api_client, data) for data in imports]
    await asyncio.gather(*corus)
    # for c in corus:
    #     await c
    ex_tree = fake_cloud.get_tree(fake_cloud[0].id)
    r_tree = await get_node(api_client, fake_cloud[0].id)
    r_tree['children'][0]['children'][0]['children'] = None
    ex_tree['children'][0]['children'][0]['children'] = None
    print(ex_tree)
    print(r_tree)
    compare_db_fc_state(sync_connection, fake_cloud)
