from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
# todo: install python 3.11 and use Self
from typing import Iterable, Mapping, Any, TypeVar  # , Self

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
