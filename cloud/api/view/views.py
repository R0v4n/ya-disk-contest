from datetime import datetime

from aiohttp.web_response import Response, json_response

from .base import BaseView
from .payloads import dumps
from ..model import ImportData, ImportModel, NodeModel
from ..model.history_model import HistoryModel


class ImportsView(BaseView):
    URL_PATH = '/imports'

    async def post(self, data: ImportData):
        async with self.pg.transaction() as conn:
            mdl = ImportModel(data, conn)
            await mdl.init()
            await mdl.just_do_it()
        return Response()


class NodeView(BaseView):
    URL_PATH = r'/nodes/{node_id}'

    async def get(self, node_id: str, /):
        async with self.pg.transaction() as conn:
            mdl = NodeModel(node_id, conn)
            node = await mdl.get_node()

        return json_response(node, dumps=dumps)


class DeleteNodeView(BaseView):
    URL_PATH = r'/delete/{node_id}'

    # todo: date validation?
    async def delete(self, node_id: str, /, date: datetime):

        async with self.pg.transaction() as conn:
            mdl = NodeModel(node_id, conn, date)
            await mdl.delete_node()

        return Response()


class UpdatesView(BaseView):
    URL_PATH = r'/updates'

    async def get(self, date: datetime):
        mdl = HistoryModel(self.pg, date)
        nodes = await mdl.get_files_updates_24h()

        return json_response({'items': nodes}, dumps=dumps)


class NodeHistoryView(BaseView):
    URL_PATH = r'/node/{node_id}/history'

    # noinspection PyPep8Naming
    async def get(self, node_id: str, /, dateStart: datetime, dateEnd: datetime):
        async with self.pg.transaction() as conn:

            mdl = NodeModel(node_id, conn)
            nodes = await mdl.get_node_history(dateStart, dateEnd)

        return json_response({'items': nodes}, dumps=dumps)