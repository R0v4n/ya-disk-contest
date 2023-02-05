from datetime import datetime
from inspect import Parameter

from asyncpgsa import PG
from fastapi import APIRouter, status, Request, Depends
from fastapi.responses import Response, ORJSONResponse
from makefun import wraps

from cloud import model
from cloud.resources import url_paths
from cloud.services import ImportService, NodeService, NodeImportService, HistoryService


def get_pg(request: Request) -> PG:
    return request.app.state.pg


def service_depends(service_class):
    @wraps(
        service_class.__init__,
        remove_args=('pg', 'self'),
        append_args=Parameter('pg', Parameter.POSITIONAL_OR_KEYWORD,
                              default=Depends(get_pg), annotation=PG)
    )
    def init_service(*args, **kwargs):
        return service_class(*args, **kwargs)

    return Depends(init_service)


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


@router.post(url_paths.IMPORTS, response_class=Response)
async def imports(service: ImportService = service_depends(ImportService)):
    await service.execute_post_import()
    return Response()


@node_router.delete(
    url_paths.DELETE_NODE,
    response_class=Response,
)
async def delete_node(service: NodeImportService = service_depends(NodeImportService)):
    await service.execute_delete_node()
    return Response()


@node_router.get(
    url_paths.GET_NODE,
    response_model=model.ResponseNodeTree,
    response_class=ORJSONResponse
)
async def node_tree(service: NodeService = service_depends(NodeService)):
    tree = await service.get_node()
    return ORJSONResponse(tree.dict(by_alias=True))


@router.get(
    url_paths.GET_UPDATES,
    response_model=model.ListResponseItem,
    response_class=ORJSONResponse
)
async def updates(service: HistoryService = service_depends(HistoryService)):
    items = await service.get_files_updates()
    return ORJSONResponse(items.dict(by_alias=True))


# noinspection PyPep8Naming
@node_router.get(
    url_paths.GET_NODE_HISTORY,
    response_model=model.ListResponseItem,
    response_class=ORJSONResponse
)
async def node_history(
        dateStart: datetime,
        dateEnd: datetime,
        service: NodeService = service_depends(NodeService),
):
    items = await service.get_node_history(dateStart, dateEnd)
    return ORJSONResponse(items.dict(by_alias=True))


router.include_router(node_router)
