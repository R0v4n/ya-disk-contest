from __future__ import annotations

from datetime import datetime
from enum import Enum

import pydantic as pdt
from pydantic import Extra


class ParentIdValidationError(ValueError):
    pass


class NodeType(Enum):
    FILE = 'FILE'
    FOLDER = 'FOLDER'


class Node(pdt.BaseModel):
    id: pdt.constr(min_length=1)
    parent_id: pdt.constr(min_length=1) | None = pdt.Field(alias='parentId')
    type: NodeType
    url: pdt.constr(min_length=1, max_length=255) | None = None
    size: pdt.conint(gt=0) | None = None

    class Config:
        allow_population_by_field_name = True


class ImportNode(Node):
    class Config:
        FOLDER_DB_FIELDS = {'id', 'parent_id'}
        FILE_DB_FIELDS = {'id', 'parent_id', 'url', 'size'}

        # todo: do i need it?
        # extra = pdt.Extra.forbid

    @pdt.root_validator
    def check_fields(cls, values):
        t = values.get('type')
        url = values.get('url')
        size = values.get('size')
        none_count = (url is None) + (size is None)

        if t == NodeType.FOLDER and none_count != 2:
            raise ValueError('Folder size and url should be None')

        if t == NodeType.FILE and none_count != 0:
            raise ValueError("File size and url shouldn't be None")

        return values

    def db_dict(self, import_id: int):
        # todo: import id can be passed via param in query probably
        return self.dict(include=self.db_fields_set(self.type)) | {'import_id': import_id}

    @classmethod
    def db_fields_set(cls, node_type: NodeType):
        return cls.Config.FOLDER_DB_FIELDS if node_type == NodeType.FOLDER else cls.Config.FILE_DB_FIELDS


class ExportNode(Node):
    # todo: think about validation
    date: datetime
    size: pdt.conint(ge=0)


class ImportData(pdt.BaseModel):
    items: list[ImportNode]
    date: datetime = pdt.Field(alias='updateDate')

    class Config:
        extra = Extra.forbid

    @pdt.validator('items')
    def check_unique(cls, items):
        if len(items) > len({item.id for item in items}):
            raise ValueError("ids should be unique in one import.")

        return items


# todo: how to use it?
class Error(pdt.BaseModel):
    error: str
