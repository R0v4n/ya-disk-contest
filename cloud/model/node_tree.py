from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Iterable, Mapping, Any, TypeVar

from .data_classes import RequestItem, ItemType, ResponseItem, Item


# todo: how to deal with type hints here?
class TreeMixin(ABC):

    def __init__(self, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def construct(cls, **kwargs) -> TreeT:
        """method of pydantic.BaseModel"""

    @classmethod
    def from_records(cls, records: Iterable[Mapping[str, Any]]) -> list[TreeT]:
        nodes = (cls(**rec) for rec in records)
        return cls.from_nodes(nodes)

    @classmethod
    def from_nodes(cls, nodes: Iterable[Item | TreeT]) -> list[TreeT]:

        id_children_map: dict[str | None, list[TreeT]] = defaultdict(list)

        ids = set()
        for node in nodes:
            if type(node) != cls:
                node = cls.construct(**node.dict(by_alias=True))

            ids |= {node.id}

            if node.type == ItemType.FOLDER:
                node.children = id_children_map[node.id]

            id_children_map[node.parent_id].append(node)

        outer_ids = set(id_children_map) - ids
        top_nodes = sum((children for id_, children in id_children_map.items() if id_ in outer_ids), [])
        return top_nodes


class ResponseNodeTree(ResponseItem, TreeMixin):
    children: list[ResponseNodeTree] | None = None


class RequestNodeTree(RequestItem, TreeMixin):
    children: list[RequestNodeTree] | None = None

    def flatten_nodes_dict_gen(self, import_id: int):
        """
        Return Generator[dict] for insertion records in db.
        Children field not included.
        Any child always will be after his parent.
        """

        def get_dict(node: RequestNodeTree):
            yield node.db_dict(import_id)
            if node.children:
                for child in node.children:
                    yield from get_dict(child)

        return get_dict(self)


TreeT = TypeVar('TreeT', ResponseNodeTree, RequestNodeTree)

