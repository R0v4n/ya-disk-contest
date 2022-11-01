from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
# todo: install python 3.11 and use Self
from typing import Iterable, Mapping, Any, TypeVar  # , Self

from asyncpg import Record
from devtools import debug
# todo: fix imports path everywhere
from cloud.api.model.data_classes import ImportNode, NodeType, ExportNode, Node


TNodeModel = TypeVar('TNodeModel', bound=Node)

# todo: think about Tree interface


class TreeMixin(ABC):

    def __init__(self, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def construct(cls, **kwargs) -> 'Self':
        """method of pydantic.BaseModel"""

    @classmethod
    def from_records(cls, records: Iterable[Mapping[str, Any]]) -> list[TNodeModel]:

        nodes = (cls(**rec) for rec in records)

        return cls.from_nodes(nodes)

    @classmethod
    def from_nodes(cls, nodes: Iterable[Node | 'Self']) -> list['Self']:

        id_children_map: dict[str | None, list['Self']] = defaultdict(list)

        ids = set()
        for node in nodes:
            # fixme: how to improve this?
            if type(node) != cls:
                node = cls.construct(**node.dict(by_alias=True))

            ids |= {node.id}

            if node.type == NodeType.FOLDER:
                node.children = id_children_map[node.id]

            id_children_map[node.parent_id].append(node)

        outer_ids = set(id_children_map) - ids
        top_nodes = sum((children for id_, children in id_children_map.items() if id_ in outer_ids), [])
        return top_nodes


class ExportNodeTree(ExportNode, TreeMixin):
    children: list[ExportNodeTree] | None = None


class ImportNodeTree(ImportNode, TreeMixin):
    children: list[ImportNodeTree] | None = None

    def flatten_nodes_dict_gen(self, import_id: int):
        """
        Return Generator[dict] for insertion records in db.
        Children field not included.
        Any child always will be after his parent.
        """

        def get_dict(node: ImportNodeTree):
            yield node.db_dict(import_id)
            if node.children:
                for child in node.children:
                    yield from get_dict(child)

        return get_dict(self)


if __name__ == '__main__':
    from unit_test import EXPECTED_TREE

    d = {
        'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
        'parent_id': None,
        'type': NodeType.FILE,
        'size': 10,
        'url': 'apch-hi'
    }
    tree = ImportNodeTree.from_records([d])[0]

    debug(tree.db_dict(1))
    debug(tree.flatten_nodes_dict_gen(1))
    # exit()


    def this_is_tree():
        tree = ExportNodeTree(**EXPECTED_TREE)
        debug(tree.dict())
        # debug(tree.flatten_nodes_dict_gen(1))
        # with open('../model_scratch.json', mode='w') as f:
        #     f.write(tree.json(indent=4))


    this_is_tree()
