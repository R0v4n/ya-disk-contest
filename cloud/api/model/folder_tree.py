from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from functools import reduce
from pprint import pprint
from typing import AsyncGenerator, Iterable

from asyncpg import Record

# todo: fix imports path everywhere
from cloud.api.model.data_classes import Folder, File, ImportNode, Node, NodeType


class NodeTree(ImportNode):
    date: datetime | None
    children: list[NodeTree] | None = None

    @classmethod
    def from_records(cls, records: Iterable[Record]) -> list[NodeTree]:
        # fixme: this is bad condition for NodeType.
        nodes = (
            cls(
                type=NodeType.FILE if rec.get('url') else NodeType.FOLDER,
                **rec
            )
            for rec in records
        )

        return cls.from_nodes(nodes)

    @classmethod
    def from_nodes(cls, nodes: Iterable[ImportNode] | Iterable[Folder]) -> list[NodeTree]:

        id_children_map: dict[str | None, list[NodeTree]] = defaultdict(list)

        ids = set()
        for node in nodes:
            # fixme: how to improve this?
            if type(node) == ImportNode:
                node = cls(**node.dict(by_alias=True))

            if type(node) == Folder:
                node = cls(**(node.dict(by_alias=True) | {'type': NodeType.FOLDER}))

            ids |= {node.id}

            if node.type == NodeType.FOLDER:
                node.children = id_children_map[node.id]

            id_children_map[node.parent_id].append(node)

        outer_ids = set(id_children_map) - ids
        top_nodes = sum((children for id_, children in id_children_map.items() if id_ in outer_ids), [])
        return top_nodes

    def flatten_nodes_dict_gen(self):
        """any child always will be after his parent"""

        def get_dict(node: NodeTree):
            yield node.dict(exclude={'children', 'date', 'url', 'type'})
            if node.children:
                for child in node.children:
                    yield from get_dict(child)

        return get_dict(self)


if __name__ == '__main__':
    from unit_test import EXPECTED_TREE
    from devtools import debug

    d = {'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1', 'parent_id': None, 'type': NodeType.FOLDER}
    tree = NodeTree(**d)
    exit()


    def test_tree():
        tree = NodeTree(**EXPECTED_TREE)
        debug(tree.dict())
        # debug(tree.flatten_nodes_dict_gen())
        # with open('../model_scratch.json', mode='w') as f:
        #     f.write(tree.json(indent=4))


    test_tree()
