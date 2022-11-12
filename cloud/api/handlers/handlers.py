from datetime import datetime
from typing import Optional, Union

from aiohttp.web_response import Response, json_response
from aiohttp_pydantic.oas.typing import r200, r404

from .base import BasePydanticView
from .payloads import dumps
from ..model import ImportData, ImportModel, NodeModel, ExportNodeTree, Error
from ..model.history_model import HistoryModel


class ImportsView(BasePydanticView):
    URL_PATH = '/imports'
    ModelT = ImportModel

    async def post(self, data: ImportData):
        mdl = self.ModelT(data, self.pg)
        await mdl.execute_post_import()
        return Response()


class NodeView(BasePydanticView):
    URL_PATH = r'/nodes/{node_id}'

    async def get(self, node_id: str, /) -> Union[r200[ExportNodeTree], r404[Error]]:
        node = await NodeModel(node_id, self.pg).get_node()
        return json_response(node, dumps=dumps)


class DeleteNodeView(BasePydanticView):
    URL_PATH = r'/delete/{node_id}'
    ModelT = NodeModel

    async def delete(self, node_id: str, /, date: datetime):
        await self.ModelT(node_id, self.pg, date).execute_del_node()
        return Response()


class UpdatesView(BasePydanticView):
    URL_PATH = r'/updates'

    async def get(self, date: datetime):
        mdl = HistoryModel(self.pg, date)
        nodes = await mdl.get_files_updates_24h()

        return json_response({'items': nodes}, dumps=dumps)


class NodeHistoryView(BasePydanticView):
    URL_PATH = r'/node/{node_id}/history'

    # noinspection PyPep8Naming
    async def get(self, node_id: str, /, dateStart: datetime, dateEnd: datetime):

        mdl = NodeModel(node_id, self.pg)
        nodes = await mdl.get_node_history(dateStart, dateEnd)

        return json_response({'items': nodes}, dumps=dumps)
