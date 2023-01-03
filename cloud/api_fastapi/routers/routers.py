from datetime import datetime

from asyncpgsa import pg
from fastapi import APIRouter

from cloud import model

router = APIRouter()


@router.post('/imports')
async def imports(data: model.ImportData):
    mdl = model.ImportModel(data)
    async with pg.transaction() as conn:
        await mdl.init(conn)
        await mdl.execute_post_import()


@router.delete('/delete/{node_id}')
async def del_node(node_id: str, date: datetime):
    mdl = model.NodeModel(node_id, date)
    async with pg.transaction() as conn:
        await mdl.init(conn)
        await mdl.execute_delete_node()


@router.get('/nodes/{node_id}', response_model=model.ExportNodeTree)
async def node_tree(node_id: str):
    mdl = model.NodeModel(node_id)
    await mdl.init(pg)

    return await mdl.get_node()


@router.get('/updates', response_model=model.ListExportItems)
async def updates(date: datetime):
    mdl = model.HistoryModel(pg, date)
    return await mdl.get_files_updates_24h()


# noinspection PyPep8Naming
@router.get('/node/{node_id}/history', response_model=model.ListExportItems)
async def node_history(node_id: str, dateStart: datetime, dateEnd: datetime):
    mdl = model.NodeModel(node_id)
    await mdl.init(pg)
    return await mdl.get_node_history(dateStart, dateEnd)
