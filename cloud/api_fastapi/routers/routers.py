from datetime import datetime

from asyncpgsa import PG
from fastapi import APIRouter, status, Request, Depends
from fastapi.responses import Response

from cloud import model

router = APIRouter()


def get_pg(request: Request) -> PG:
    return request.app.state.pg


@router.post('/imports')
async def imports(mdl: model.ImportModel = Depends(), pg: PG = Depends(get_pg)):
    async with pg.transaction() as conn:
        await mdl.init(conn)
        await mdl.execute_post_import()


@router.delete(
    '/delete/{node_id}',
    response_class=Response,
    responses={
        status.HTTP_200_OK: {'description': 'Success'},
        status.HTTP_400_BAD_REQUEST: {'model': model.Error},
        status.HTTP_404_NOT_FOUND: {'model': model.Error},
    }
)
async def delete_node(mdl: model.NodeModel = Depends(), pg: PG = Depends(get_pg)):
    async with pg.transaction() as conn:
        await mdl.init(conn)
        await mdl.execute_delete_node()
    return Response()


@router.get('/nodes/{node_id}', response_model=model.ResponseNodeTree)
async def node_tree(mdl: model.NodeModel = Depends(), pg: PG = Depends(get_pg)):
    await mdl.init(pg)
    return await mdl.get_node()


@router.get('/updates', response_model=model.ListResponseItem)
async def updates(date: datetime, pg: PG = Depends(get_pg)):
    mdl = model.HistoryModel(pg, date)
    return await mdl.get_files_updates_24h()


# noinspection PyPep8Naming
@router.get('/node/{node_id}/history', response_model=model.ListResponseItem)
async def node_history(
        dateStart: datetime,
        dateEnd: datetime,
        mdl: model.NodeModel = Depends(),
        pg: PG = Depends(get_pg)):

    await mdl.init(pg)
    return await mdl.get_node_history(dateStart, dateEnd)
