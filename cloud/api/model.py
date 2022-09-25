from __future__ import annotations

from datetime import datetime
from enum import unique, Enum
from pprint import pprint

import pydantic as pdt


@unique
class NodeType(Enum):
    FILE = 'FILE'
    FOLDER = 'FOLDER'


class NodeModel(pdt.BaseModel):
    id: str
    parent_id: str | None = pdt.Field(alias='parentId')
    size: pdt.conint(ge=0) = 0


# type1: NodeType

# @pdt.root_validator()


class FileModel(NodeModel):
    url: pdt.constr(min_length=1, max_length=255)


class FolderModel(NodeModel):
    pass


class NodeTreeModel(FileModel):
    type: NodeType
    date: datetime
    url: pdt.constr(min_length=1, max_length=255) | None
    children: list[NodeTreeModel] | None


class ImportModel(pdt.BaseModel):
    items: list[FileModel | FolderModel]
    date: datetime = pdt.Field(alias='updateDate')


if __name__ == '__main__':
    from unit_test import IMPORT_BATCHES, EXPECTED_TREE


    def test_import_model():
        for b in IMPORT_BATCHES:
            try:
                im = ImportModel(**b)
                print(type(im))
                for i in im.items:
                    print(type(i))
                    print(i.json())
                print()
            except pdt.ValidationError as e:
                print(e.json())
                break


    def test_tree():
        tree = NodeTreeModel(**EXPECTED_TREE)
        # pprint(tree.dict())
        with open('model_scratch.json', mode='w') as f:
            f.write(tree.json(indent=4))


    test_tree()
