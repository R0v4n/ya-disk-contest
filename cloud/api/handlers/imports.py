from typing import Generator
import dateutil.parser

from aiohttp.web_response import Response

from .base import BaseView
from ...db.schema import imports, nodes


class ImportsView(BaseView):
    URL_PATH = '/imports'

    @staticmethod
    def nodes_gen(items: list[dict], import_id: int) -> Generator:

        for item in items:
            yield {
                'import_id': import_id,
                'node_id': item['id'],
                'parent_id': item['parentId'],
                'url': item.get('url'),
                'size': item.get('size'),
                'type': item['type'],
                'is_actual': True
            }

    async def post(self):
        async with self.pg.transaction() as conn:
            data = await self.request.json()
            update_date = dateutil.parser.parse(data['updateDate'])

            query = imports.insert().values({'update_date': update_date}).returning(imports.c.import_id)
            import_id = await conn.fetchval(query)

            query = nodes.insert()
            await conn.execute(query.values(list(self.nodes_gen(data['items'], import_id))))

        return Response()
