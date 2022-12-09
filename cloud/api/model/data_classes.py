from __future__ import annotations

from datetime import datetime
from enum import Enum

import pydantic as pdt
from pydantic import Extra


class ParentIdValidationError(ValueError):
    pass


class ItemType(Enum):
    FILE = 'FILE'
    FOLDER = 'FOLDER'


class Item(pdt.BaseModel):
    id: pdt.constr(min_length=1)
    parent_id: pdt.constr(min_length=1) | None = pdt.Field(alias='parentId')
    type: ItemType
    url: pdt.constr(min_length=1, max_length=255) | None = None
    size: pdt.conint(gt=0) | None = None

    class Config:
        allow_population_by_field_name = True


class ImportItem(Item):
    class Config:
        FOLDER_DB_FIELDS = {'id', 'parent_id'}
        FILE_DB_FIELDS = {'id', 'parent_id', 'url', 'size'}

        extra = pdt.Extra.forbid

    @pdt.root_validator
    def check_fields(cls, values):
        t = values.get('type')
        url = values.get('url')
        size = values.get('size')
        none_count = (url is None) + (size is None)

        if t == ItemType.FOLDER and none_count != 2:
            raise ValueError('Folder size and url should be None')

        if t == ItemType.FILE and none_count != 0:
            raise ValueError("File size and url shouldn't be None")

        return values

    def db_dict(self, import_id: int):
        return self.dict(include=self.db_fields_set(self.type)) | {'import_id': import_id}

    @classmethod
    def db_fields_set(cls, node_type: ItemType):
        return cls.Config.FOLDER_DB_FIELDS if node_type == ItemType.FOLDER else cls.Config.FILE_DB_FIELDS


class ExportItem(Item):
    date: datetime
    size: pdt.conint(ge=0)


class ImportData(pdt.BaseModel):
    items: list[ImportItem]
    date: datetime = pdt.Field(alias='updateDate')

    class Config:
        extra = Extra.forbid

    @pdt.validator('items')
    def check_unique(cls, items):
        if len(items) > len({item.id for item in items}):
            raise ValueError("ids should be unique in one import.")

        return items


class Error(pdt.BaseModel):
    code: int
    message: str
