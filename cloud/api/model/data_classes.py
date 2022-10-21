from __future__ import annotations

from datetime import datetime
from enum import unique, Enum

import pydantic as pdt


@unique
class NodeType(Enum):
    FILE = 'FILE'
    FOLDER = 'FOLDER'


class Node(pdt.BaseModel):
    id: str
    parent_id: str | None = pdt.Field(alias='parentId')


class ImportNode(Node):
    type: NodeType
    url: pdt.constr(min_length=1, max_length=255) | None = None
    size: pdt.conint(gt=0) | None = None

    class Config:
        extra = pdt.Extra.forbid
        allow_population_by_field_name = True


class ExportNode(ImportNode):
    date: datetime
    size: pdt.conint(ge=0)


class File(Node):
    url: pdt.constr(min_length=1, max_length=255)
    size: pdt.conint(gt=0)


class Folder(Node):
    pass


class ImportData(pdt.BaseModel):
    items: list[File | Folder]
    date: datetime = pdt.Field(alias='updateDate')
