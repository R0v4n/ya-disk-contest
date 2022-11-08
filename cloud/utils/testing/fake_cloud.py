from __future__ import annotations

import random
import random as rnd
from datetime import datetime, timezone, timedelta
from itertools import groupby
from typing import Any
from uuid import UUID

import faker
import pydantic as pdt
from devtools import debug

from cloud.api.model import NodeType

fake = faker.Faker(use_weighting=False)


class Item(pdt.BaseModel):
    # without lambda its generates same ids for first file and first folder, etc. seed is constant somehow? or wtf?
    id: str = pdt.Field(default_factory=lambda: fake.uuid4())
    parent_id: str | None = pdt.Field(default=None, alias='parentId')

    import_id: int | None = None
    date: datetime | None = None

    # defined in derived classes
    type: None
    url: None
    size: None
    children: None

    class Config:
        # defined in derived classes
        updates_allowed: set[str]
        db_fields: set[str]
        import_fields: set[str]

        # use_enum_values = True
        allow_population_by_field_name = True
        fields = {'import_id': {'exclude': True}}

    @property
    def import_dict(self):
        return self.dict(include=self.Config.import_fields, by_alias=True)

    @property
    def export_dict(self):
        d = self.dict(exclude={'children', 'import_id'}, by_alias=True)
        d['date'] = str(self.date)
        return d

    def tree_dict(self, nullify_folder_sizes=False):
        tree = self.dict(by_alias=True)
        self._cast_tree_types(tree, nullify_folder_sizes)
        return tree

    @staticmethod
    def _cast_tree_types(tree: dict[str, Any], nullify_folder_sizes=False):
        def cast(node_dict: dict[str, Any]):
            node_dict['date'] = str(node_dict['date'])
            if node_dict['type'] == NodeType.FOLDER.value:
                if nullify_folder_sizes:
                    node_dict['size'] = 0
                for node in node_dict['children']:
                    cast(node)

        cast(tree)

    def raw_db_dict(self, field_names_mapping: dict[str, str] = None) -> dict[str, Any]:
        # import id excluded in Config. This is the only way to exclude it in recursive tree dict.
        # (except for the manual excluding). In this case, import_id will not be included in the dict,
        # even if specified below in include set.
        d = self.dict(include=self.Config.db_fields) | {'import_id': self.import_id}
        if field_names_mapping:
            for old_key, new_key in field_names_mapping.items():
                val = d.pop(old_key)
                d[new_key] = val

        return d

    def update(self, **fields):
        """No validation, only allowed field check. Values should be trusted"""
        forbidden_keys = set(fields.keys()) - self.Config.updates_allowed
        if forbidden_keys:
            raise ValueError(f'Invalid keys: {tuple(forbidden_keys)}. '
                             f'Allowed are: {tuple(self.Config.updates_allowed)}')

        for key, val in fields.items():
            setattr(self, key, val)

    def shallow_copy(self):
        children_default = None if type(Item) == File else []
        return self.copy(update={'children': children_default, 'import_id': self.import_id})

    @property
    def children_ids(self):
        return set()


class Folder(Item):
    type: str = NodeType.FOLDER.value
    url: None = None
    size: pdt.conint(ge=0) | None = 0

    children: list[Folder | File] = pdt.Field(default_factory=list)

    class Config(Item.Config):
        updates_allowed = {'parent_id'}
        db_fields = {'import_id', 'id', 'parent_id', 'size'}
        import_fields = {'id', 'parent_id', 'type', 'url'}  # size=None added in import_dict prop

    @property
    def import_dict(self):
        return super().import_dict | {'size': None}

    @property
    def children_ids(self):
        def yield_children_ids(children: list[Item]):
            for child in children:
                yield child.id
                if type(child) == Folder:
                    yield from yield_children_ids(child.children)

        return set(yield_children_ids(self.children))


class File(Item):
    type: str = NodeType.FILE.value
    url: str = pdt.Field(default_factory=lambda: fake.file_path())
    size: pdt.conint(gt=0) = pdt.Field(default_factory=lambda: fake.random_int(1, 10))

    children: None = None

    class Config(Item.Config):
        updates_allowed = {'parent_id', 'size', 'url'}
        db_fields = {'import_id', 'id', 'parent_id', 'size', 'url'}
        import_fields = {'id', 'parent_id', 'type', 'url', 'size'}


class Import(pdt.BaseModel):
    id: int
    items: dict[str, Item] = pdt.Field(default_factory=dict)
    date: datetime
    history_ids: set[str] = pdt.Field(default_factory=set)

    def add_item(self, item: Item):
        item = item.shallow_copy()
        self.items[item.id] = item


# todo: write tests for it!
class FakeCloud:
    faker = fake

    def __init__(self):

        self._root = Folder.construct(id=None)
        self._items: dict[str | None, Item] = {None: self._root}
        self._imports: list[Import] = []
        self._history: list[Item] = []

    def generate_import(
            self,
            *tree_schemas: list[list | int] | int,
            date: datetime | str = None,
            parent_id: str | None = None,
            is_new=True) -> int:
        """
        :param tree_schemas:
            list representing folder, int - number of files ([] - empty folder, [2] - folder with two files).
            e.g. generate_trees(2, [1,[3]]) will generate 2 files with parent_id = param parent_id,
            "folder1" with parent_id = param parent_id,
            file with parent_id = folder1.id,
            "folder2" with parent_id = folder1.id,
            3 files with parent_id = folder2.id.
        :param date:
            import datetime. If None, "utc now" is used
            (or last import date + 1 sec if last import datetime is ge than datetime.now).
        :param parent_id:
            parent id for top nodes in each tree schema.
        :param is_new:
            if True generates new import, otherwise appends last import (date param is not used in second case).

        :return import id

        """
        if is_new:
            self._add_new_import_obj(date)

        date = self._imports[-1].date
        import_id = self._imports[-1].id

        def build(schema_item, parent_id: UUID | str):

            if isinstance(schema_item, int):
                total_size = 0
                for _ in range(schema_item):
                    f = File(parent_id=parent_id, date=date, import_id=import_id)
                    self._insert_new_item(f)
                    total_size += f.size
                return total_size

            elif isinstance(schema_item, list):
                f = Folder(parent_id=parent_id, date=date, import_id=import_id)
                self._insert_new_item(f)
                f.size = sum(build(i, f.id) for i in schema_item)
                return f.size

            else:
                raise TypeError('Invalid schema item type (allowed list or int)')

        if tree_schemas:
            size = sum(build(schema, parent_id) for schema in tree_schemas)
            self._update_parents(parent_id, size)

        return self._imports[-1].id

    def load_import(self, import_data: dict):
        # warning: there are no check for folder order in import and item validation on insert and update.
        date = import_data['updateDate']
        import_id = self._add_new_import_obj(date)
        items = import_data['items']

        for item_dict in items:
            item_type = Folder if item_dict['type'] == NodeType.FOLDER.value else File
            f = item_type(**item_dict, date=date, import_id=import_id)

            if f.size is None:
                f.size = 0

            if f.id in self._items:
                self.update_item(f.id, **f.dict(include=f.Config.updates_allowed))
            else:
                self._insert_new_item(f)
                self._update_parents(f.parent_id, f.size)

    def insert_item(self, item: Item):
        item = item.copy()

        if item.id in self.ids:
            raise ValueError('Item already exist.')

        item.date = self._imports[-1].date
        item.import_id = self._imports[-1].id

        self._insert_new_item(item)
        self._update_parents(item.parent_id, item.size)

    def update_item(self, id_: str, **fields: Any):
        item = self._items.get(id_)
        if item is None or id_ is None:
            raise ValueError('Item does not exist.')

        if item.id in self._imports[-1].items:
            raise ValueError('Item can not be imported twice in one import.')

        # note: if delta size is 0 or even nothing changed at all (except date),
        #  it still writes parents to history. this is how the database works at the moment
        self._update_parents(item.parent_id, -item.size)
        self._pop_from_children(item)

        self._write_item_to_history(item)

        item.update(**fields)
        item.date = self._imports[-1].date
        item.import_id = self._imports[-1].id

        self._items[item.parent_id].children.append(item)
        self._update_parents(item.parent_id, item.size)

        self._imports[-1].add_item(item)

    def get_tree(self, node_id=None, nullify_folder_sizes=False) -> dict[str, Any]:
        return self._items[node_id].tree_dict(nullify_folder_sizes)

    def del_item(self, id_: str, date: datetime = None):

        self._add_new_import_obj(date)

        item = self._get_item(id_)
        self._update_parents(item.parent_id, -item.size)
        deleted_ids = item.children_ids | {item.id}
        for i in deleted_ids:
            self._items.pop(i)

        self._pop_from_children(item)

        # warning: O(N)
        for i in range(len(self._history) - 1, -1, -1):
            if self._history[i].id in deleted_ids:
                self._history.pop(i)

        return self._imports[-1].date

    def get_node_history(self, id_: str, date_start: datetime, date_end: datetime):
        item = self._get_item(id_)

        items = [
            item.export_dict for item in self._history + [item]
            if item.id == id_ and date_start <= item.date < date_end
        ]

        return {'items': items}

    def get_all_history(self):
        return [item.export_dict for item in self._history]

    def get_updates(self, date_start: datetime = None, date_end: datetime = None):
        date_end = date_end or datetime.now(timezone.utc)

        if date_start is None:
            date_start = date_end - timedelta(days=1)

        items = sorted(
            filter(
                lambda item: item.type == NodeType.FILE.value and date_start <= item.date <= date_end,
                self._history + list(self._items_gen)
            ),
            key=lambda item: item.id
        )

        items = [
            max(g, key=lambda item: item.date).export_dict
            for k, g in groupby(items, key=lambda item: item.id)
        ]

        return {'items': items}

    def get_import_dict(self, import_id: int = None):
        """
        first import always has id = 1.
        :param import_id: if None returns last import data. Can be negative value (-1 is last import data)
        :return: import data dict formatted according to API spec.
        """
        data = self._get_import_data(import_id)
        return {
            'items': [item.import_dict for item in data.items.values()],
            'updateDate': str(data.date)
        }

    def imports_gen(self):
        return (self.get_import_dict(i.id) for i in self._imports)

    def get_node_copy(self, path: str) -> Item:

        path_elements = path.split('/')
        if path_elements[0] == '':
            path_elements = path_elements[1:]

        node: Item | None = None

        def find_node(nodes: list[Item], names: list[str]):
            nonlocal node

            name = names[0]
            num = int(name[1])
            searching_type = Folder if name[0] == 'd' else File

            c = 0
            for i in nodes:
                if type(i) == searching_type:
                    c += 1
                if c == num:
                    if searching_type == Folder:
                        if len(names) > 1:
                            find_node(i.children, names[1:])
                        else:
                            node = i
                    else:
                        node = i
                    break
            else:
                raise ValueError('Path does not exist.')

        find_node(self._root.children, path_elements)

        node = node.shallow_copy()
        return node

    def __getitem__(self, item: int | tuple[int, ...]):
        if isinstance(item, int):
            item = item,

        it = iter(item)

        node = self._root.children[next(it)]

        for i in it:
            try:
                node = node.children[i]
            except (IndexError, TypeError):
                raise IndexError('Node does not exist.')

        return node.shallow_copy()

    def get_raw_db_imports_records(self) -> list[dict[str, str]]:
        return [i.dict(include={'id', 'date'}) for i in self._imports]

    def get_raw_db_node_records(self):
        # more-itertools.partition
        files_gen = (item.raw_db_dict()
                     for item in self._items_gen if type(item) == File)
        folders_gen = (item.raw_db_dict()
                       for item in self._items_gen if type(item) == Folder)

        return list(files_gen), list(folders_gen)

    def get_raw_db_history_records(self):
        files_gen = (item.raw_db_dict({'id': 'file_id'})
                     for item in self._history if type(item) == File)
        folders_gen = (item.raw_db_dict({'id': 'folder_id'})
                       for item in self._history if type(item) == Folder)

        return list(files_gen), list(folders_gen)

    @property
    def ids(self):
        return tuple(key for key in self._items.keys() if key is not None)

    @property
    def last_import_date(self):
        if not self._imports:
            return None

        return self._imports[-1].date

    def _pop_from_children(self, child: Item):
        parent = self._items[child.parent_id]
        ind = parent.children.index(child)
        parent.children.pop(ind)

    def _get_item(self, id_: str):
        item = self._items.get(id_)
        if item is None or item.id is None:
            raise ValueError(f'Item with id {id_} does not exist.')
        return item

    @property
    def _items_gen(self):
        return (item for id_, item in self._items.items() if id_)

    def _add_new_import_obj(self, date: datetime | None):
        if date is None:
            date = datetime.now(timezone.utc)
            # warning: this can be an issue in some cases
            #  (e.g. some datetime.now added manually, after generating future datetime)
            if self._imports and self._imports[-1].date >= date:
                date = self._imports[-1].date + timedelta(seconds=1)

        self._imports.append(Import(id=len(self._imports) + 1, date=date))

        if len(self._imports) > 1 and self._imports[-2].date >= self._imports[-1].date:
            raise ValueError('The new import date must be greater than previous one.')

        return self._imports[-1].id

    def _get_import_data(self, import_id: int = None) -> Import:
        if import_id is None:
            import_id = -1

        if import_id > len(self._imports) or import_id == 0 or import_id < - len(self._imports):
            raise ValueError(f'Import with id = {import_id} does not exist')
        else:
            if import_id > 0:
                import_id -= 1
            return self._imports[import_id]

    def _insert_new_item(self, item: Item):
        self._items[item.id] = item
        self._items[item.parent_id].children.append(item)
        self._imports[-1].add_item(item)

    def _update_parents(self, parent_id: str, size: int):
        date = self._imports[-1].date
        import_id = self._imports[-1].id

        def update(i: str, depth=0):
            nonlocal size, parent_id

            if depth > 0 and i == parent_id:
                raise ValueError(f'Circular node links detected. Starting parent_id={parent_id}')

            item = self._items[i]

            if item.import_id != import_id:
                self._write_item_to_history(item)
                item.date = date
                item.import_id = import_id

            item.size += size

            if item.id is not None:
                update(item.parent_id, depth + 1)

        update(parent_id)

    # todo: use TypeVar or Self with 3.11
    def _write_item_to_history(self, item: Item):
        history_ids = self._imports[-1].history_ids
        if item.id is not None and item.id not in history_ids:
            history_ids |= {item.id}

            self._history.append(item.shallow_copy())


class FakeCloudGen(FakeCloud):
    # todo: use faker instance

    @property
    def _folder_ids(self):
        """None included!"""
        return tuple(key for key, val in self._items.items() if type(val) == Folder)

    @staticmethod
    def random_schema(max_files_in_one_folder=5, max_depth=None):
        def build(depth=1):
            if max_depth is None or depth < max_depth:
                coin = rnd.choice(range(2))
            else:
                coin = 0

            if coin:
                return [build(depth + 1) for i in range(rnd.randint(0, 2))]
            else:
                return rnd.randint(1, max_files_in_one_folder)

        res = build()

        return [res] if isinstance(res, int) else res

    def random_import(self, *, schemas_count=1, allow_random_count=True, max_files_in_one_folder=5):
        self.generate_import()

        if allow_random_count:
            schemas_count = rnd.randint(0, schemas_count)

        for _ in range(schemas_count):
            self.generate_import(
                *self.random_schema(max_files_in_one_folder=max_files_in_one_folder),
                parent_id=rnd.choice(self._folder_ids),
                is_new=False
            )

    def random_update(self):
        ids = set(self.ids) - self._imports[-1].items.keys()
        id_ = rnd.choice(tuple(ids))
        item = self._items[id_]

        allowed_parents = set(self._folder_ids)

        if type(item) == Folder:
            allowed_parents -= item.children_ids | {id_}
            self.update_item(id_, parent_id=random.choice(tuple(allowed_parents)))
        else:
            updates = [
                ('parent_id', random.choice(tuple(allowed_parents))),
                ('size', rnd.randint(1, 10)),
                ('url', fake.file_path())
            ]

            updates = rnd.choices(updates, k=rnd.randint(1, 3))
            self.update_item(id_, **dict(updates))

    def random_updates(self, *, count=2, allow_random_count=True):
        items_count = len(self._items) - 1 - len(self._imports[-1].items)

        if allow_random_count:
            count = rnd.randint(0, count)

        count = min(items_count, count)

        for _ in range(count):
            self.random_update()

    def random_del(self):
        ids = self.ids
        if ids:
            i: str = rnd.choice(self.ids)
            return i, self.del_item(i)
        else:
            return None, None


if __name__ == '__main__':
    def some_generation():
        fc = FakeCloud()
        fc.generate_import([2], [1], 1)
        # debug(fc.get_tree(nullify_folder_sizes=True))
        # debug(fc.get_import_dict())
        # time.sleep(0.1)
        d2 = fc.get_node_copy('d2')
        fc.generate_import([2, [1]], parent_id=d2.id)
        f1 = fc.get_node_copy('d2/d1/f1')
        d3 = fc.get_node_copy('d2/d1')

        debug(fc.get_tree())
        debug(fc.get_raw_db_history_records())
        fc.del_item(d3.id)
        debug(fc.get_tree())
        debug(fc.get_raw_db_history_records())

        # debug(fc.get_raw_db_records())


    def updated_file_in_new_folder_case():
        # todo: test this case in app:
        #  on the second import, existing file is moved to the new folder.
        #  I assume that this case is impossible in real life.
        #  user can't create new folder and move existing file to it simultaneously.
        #  The correct behavior would be to not add the folder to the history, I suppose.
        cloud = FakeCloud()
        cloud.generate_import([1])
        debug(cloud.get_tree())

        folder1 = cloud.get_node_copy('d1')
        file = cloud.get_node_copy('d1/f1')

        cloud.generate_import([], parent_id=folder1.id)

        folder2 = cloud.get_node_copy('d1/d1')
        cloud.update_item(file.id, parent_id=folder2.id)

        debug(cloud.get_tree())
        debug(cloud.get_import_dict())
        debug(cloud.get_raw_db_history_records())


    def some_check():
        cloud = FakeCloud()
        cloud.generate_import([2, [2]])
        i = cloud.get_node_copy('d1/f1').id
        cloud.generate_import()
        cloud.update_item(i)
        cloud.update_item(i)
        exit()
        d1 = cloud.get_node_copy('d1/d1')
        f1 = cloud.get_node_copy('d1/f1')

        cloud.generate_import(1, parent_id=d1.id)
        cloud.update_item(f1.id, size=100)
        debug(cloud.get_updates())
        debug(cloud.get_tree())


    some_check()
