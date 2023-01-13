from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping, Any, TypeVar

from .schemas import RequestItem, ItemType, ResponseItem, Item


class NodeTree(Item):

    @classmethod
    def from_records(cls, records: Iterable[Mapping[str, Any]]) -> list[NodeTreeT]:
        nodes = (cls(**rec) for rec in records)
        return cls.from_nodes(nodes)

    @classmethod
    def from_nodes(cls, nodes: Iterable[Item | NodeTreeT]) -> list[NodeTreeT]:

        id_children_map: dict[str | None, list[NodeTreeT]] = defaultdict(list)

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


NodeTreeT = TypeVar('NodeTreeT', bound=NodeTree)


class ResponseNodeTree(ResponseItem, NodeTree):
    children: list[ResponseNodeTree] | None = None


class RequestNodeTree(RequestItem, NodeTree):
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

