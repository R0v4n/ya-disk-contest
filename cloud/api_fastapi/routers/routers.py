from datetime import datetime

from asyncpgsa import PG
from fastapi import APIRouter, status, Request, Depends
from fastapi.responses import Response

from cloud import model
from cloud.resources import url_paths

router = APIRouter(
    responses={
        status.HTTP_400_BAD_REQUEST: {'model': model.Error},
    }
)

node_router = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {'model': model.Error},
    }
)


def get_pg(request: Request) -> PG:
    return request.app.state.pg


@router.post(url_paths.IMPORTS, response_class=Response)
async def imports(mdl: model.ImportModel = Depends(), pg: PG = Depends(get_pg)):
    async with pg.transaction() as conn:
        await mdl.init(conn)
        await mdl.execute_post_import()

    return Response()


@node_router.delete(
    url_paths.DELETE_NODE,
    response_class=Response,
)
async def delete_node(mdl: model.NodeImportModel = Depends(), pg: PG = Depends(get_pg)):
    async with pg.transaction() as conn:
        await mdl.init(conn)
        await mdl.execute_delete_node()

    return Response()


@node_router.get(url_paths.GET_NODE, response_model=model.ResponseNodeTree)
async def node_tree(mdl: model.NodeModel = Depends(), pg: PG = Depends(get_pg)):
    await mdl.init(pg)
    return await mdl.get_node()


@router.get(url_paths.GET_UPDATES, response_model=model.ListResponseItem)
async def updates(mdl: model.HistoryModel = Depends(), pg: PG = Depends(get_pg)):
    await mdl.init(pg)
    return await mdl.get_files_updates()


# noinspection PyPep8Naming
@node_router.get(url_paths.GET_NODE_HISTORY, response_model=model.ListResponseItem)
async def node_history(
        dateStart: datetime,
        dateEnd: datetime,
        mdl: model.NodeModel = Depends(),
        pg: PG = Depends(get_pg)):

    await mdl.init(pg)
    return await mdl.get_node_history(dateStart, dateEnd)


router.include_router(node_router)
