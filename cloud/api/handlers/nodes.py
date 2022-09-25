from collections import defaultdict
from typing import Generator

from aiohttp.web_response import json_response
from asyncpg import Record
from asyncpgsa.pgsingleton import CursorInterface
from sqlalchemy import select

from .base import BaseView
from .payloads import dumps
from ...db.schema import nodes, imports
from ..model import NodeType


class NodesView(BaseView):
    URL_PATH = r'/nodes/{node_id}'

    # todo: read about AsyncGenJSONListPayload, need to send json without buffer
    # todo: need refactor. dataclasses or marshmallow(?)
    @staticmethod
    async def node_tree(cursor: CursorInterface):

        def node_dict(record: Record):
            return {
                'type': record['type'],
                'id': record['node_id'],
                'size': record.get('size') or 0,
                'parentId': record['parent_id'],
                'url': record.get('url'),
                'date': record['update_date'],
                'children': None
            }

        map_id_children = defaultdict(list)

        first = True
        async for row in cursor:
            node = node_dict(row)

            if first:
                top_node = node
                first = False

            if node['type'] == NodeType.FOLDER.value:
                node['children'] = map_id_children[node['id']]

            map_id_children[node['parentId']].append(node)

        return top_node

    @staticmethod
    def calculate_folder_size(folder_dict: dict):

        def run(node):
            if node['type'] == NodeType.FOLDER.value:
                # dates = [node['date']]
                # for child in node['children']:
                #     node['size'] += run(child)
                #     dates.append(child['date'])
                # node['date'] = max(dates)
                node['size'] = sum(run(child) for child in node['children'])
                node['date'] = max(child['date'] for child in node['children'])

            return node['size']

        run(folder_dict)

    # todo: refactor query
    async def get(self):
        node_id = self.request.match_info.get('node_id')

    # todo: wtf? does this query necessary to be so sophisticated?
        included_nodes = \
            select(
                [nodes.c.node_id,
                 nodes.c.parent_id,
                 nodes.c.type,
                 nodes.c.url,
                 nodes.c.size,
                 imports.c.update_date]
            ). \
            select_from(
                nodes.
                join(
                    imports,
                    nodes.c.import_id == imports.c.import_id
                )
            ). \
            where(nodes.c.node_id == node_id). \
            cte(recursive=True)

        incl_alias = included_nodes.alias()
        nodes_alias = nodes.alias()

        included_nodes = included_nodes.union_all(
            select(
                [nodes_alias.c.node_id,
                 nodes_alias.c.parent_id,
                 nodes_alias.c.type,
                 nodes_alias.c.url,
                 nodes_alias.c.size,
                 imports.c.update_date]).
            select_from(
                nodes_alias.outerjoin(
                    imports,
                    nodes_alias.c.import_id == imports.c.import_id
                )
            ).
            where(nodes_alias.c.parent_id == incl_alias.c.node_id)
        )

        query = included_nodes.select()

        async with self.pg.query(query) as cursor:
            node = await self.node_tree(cursor)

        self.calculate_folder_size(node)

        return json_response(node, dumps=dumps)
