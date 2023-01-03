from __future__ import annotations

from datetime import datetime, timezone, timedelta
from functools import partial
from itertools import groupby
from typing import Any, Callable
from uuid import UUID

import faker
import pydantic as pdt

from cloud.model import ItemType


fake = faker.Faker(use_weighting=False)
rnd = fake.random


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
            if node_dict['type'] == ItemType.FOLDER.value:
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

    def shallow_copy(self) -> Item:
        children_default = None if type(Item) == File else []
        return self.copy(update={'children': children_default, 'import_id': self.import_id})

    @property
    def children_ids(self):
        return set()


class Folder(Item):
    type: str = ItemType.FOLDER.value
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
    type: str = ItemType.FILE.value
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
    deleted_id: str | None = None

    def add_item(self, item: Item):
        item = item.shallow_copy()
        self.items[item.id] = item

    def import_dict(self):
        if self.deleted_id is None:
            return {
                'items': [item.import_dict for item in self.items.values()],
                'updateDate': str(self.date)
            }
        else:
            return {
                'deleted_id': self.deleted_id,
                'updateDate': str(self.date)
            }


class FakeCloud:
    faker = fake
    __slots__ = ('_root', '_items', '_imports', '_history', '_folder_ids')

    def __init__(self):

        self._root = Folder.construct(id=None)
        self._items: dict[str | None, Item] = {None: self._root}
        self._imports: list[Import] = []
        self._history: list[Item] = []

        self._folder_ids: set[str | None] = {None}

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

        def build(schema_item, prnt_id: UUID | str):

            if isinstance(schema_item, int):
                total_size = 0
                for _ in range(schema_item):
                    f = File(parent_id=prnt_id, date=date, import_id=import_id)
                    self._insert_new_item(f)
                    total_size += f.size
                return total_size

            if isinstance(schema_item, list):
                f = Folder(parent_id=prnt_id, date=date, import_id=import_id)
                self._insert_new_item(f)
                f.size = sum(build(i, f.id) for i in schema_item)
                return f.size

            raise TypeError('Invalid schema item type (allowed list or int)')

        if tree_schemas:
            size = sum(build(schema, parent_id) for schema in tree_schemas)
            self._update_parents(parent_id, size)

        return self._imports[-1].id

    def load_import(self, import_data: dict):
        # note: there are no check for folder order in import and item validation on insert and update.
        date = import_data['updateDate']
        import_id = self._add_new_import_obj(date)
        items = import_data['items']

        for item_dict in items:
            item_type = Folder if item_dict['type'] == ItemType.FOLDER.value else File
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

    def get_node_copy(self, node_id):
        return self._items[node_id].shallow_copy()

    def get_tree(self, node_id=None, nullify_folder_sizes=False) -> dict[str, Any]:
        return self._items[node_id].tree_dict(nullify_folder_sizes)

    def del_item(self, id_: str, date: datetime = None):

        item = self._get_item(id_)

        self._add_new_import_obj(date)
        self._imports[-1].deleted_id = item.id

        self._update_parents(item.parent_id, -item.size)
        deleted_ids = item.children_ids | {item.id}
        for i in deleted_ids:
            self._items.pop(i)

        self._folder_ids -= deleted_ids

        self._pop_from_children(item)

        # note: O(N)
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
                lambda item: item.type == ItemType.FILE.value and date_start <= item.date <= date_end,
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

        :param import_id:
            if None returns last import data. First import always has id = 1.
            Can be negative value (-1 is last import data)
        :return: import data dict formatted according to API spec.
        """
        return self._get_import_obj(import_id).import_dict()

    def imports_gen(self):
        return (self.get_import_dict(i.id) for i in self._imports)

    def __getitem__(self, item: int | tuple[int, ...]) -> Item:
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
        return tuple(self._items.keys() - {None})

    @property
    def folder_ids(self):
        return tuple(self._folder_ids - {None})

    @property
    def file_ids(self):
        return tuple(self._items.keys() - self._folder_ids)

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
            # note: this can be an issue in some cases
            #  (e.g. some datetime.now added manually, after generating future datetime)
            if self._imports and self._imports[-1].date >= date:
                date = self._imports[-1].date + timedelta(seconds=1)

        self._imports.append(Import(id=len(self._imports) + 1, date=date))

        if len(self._imports) > 1 and self._imports[-2].date >= self._imports[-1].date:
            raise ValueError(f'The new import date ({self._imports[-1].date}) '
                             f'must be greater than previous one ({self._imports[-2].date})')

        return self._imports[-1].id

    def _get_import_obj(self, import_id: int = None) -> Import:
        if import_id is None:
            import_id = -1

        if import_id > len(self._imports) or import_id == 0 or import_id < - len(self._imports):
            raise ValueError(f'Import with id = {import_id} does not exist')

        if import_id > 0:
            import_id -= 1

        return self._imports[import_id]

    def _insert_new_item(self, item: Item):
        self._items[item.id] = item
        self._items[item.parent_id].children.append(item)
        self._imports[-1].add_item(item)

        if type(item) == Folder:
            self._folder_ids.add(item.id)

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

    def _write_item_to_history(self, item: Item):
        history_ids = self._imports[-1].history_ids
        if item.id is not None and item.id not in history_ids:
            history_ids |= {item.id}

            self._history.append(item.shallow_copy())


def random_schema(max_files_in_one_folder: int = 5, max_depth: int | None = 10,
                  folder_weight: int = 1, max_branch_count: int = 2) -> list[list | int]:
    def build(depth=1):
        if max_depth is None or depth < max_depth:
            coin = rnd.choice(range(1 + folder_weight))
        else:
            coin = 0

        if coin:
            return [build(depth + 1) for _ in range(rnd.randint(0, max_branch_count))]
        else:
            return rnd.randint(1, max_files_in_one_folder)

    res = build()

    return [res] if isinstance(res, int) else res


default_schema_gen = partial(random_schema, max_files_in_one_folder=5, max_depth=10,
                             folder_weight=1, max_branch_count=2)


class FakeCloudGen(FakeCloud):

    __slots__ = ('_write_history',)

    def __init__(self, write_history=True):

        super().__init__()
        self._write_history = write_history

    def _write_item_to_history(self, item: Item):
        if self._write_history:
            super()._write_item_to_history(item)

    def random_import(
            self, *,
            schemas_count=1,
            schema_gen_func: Callable[[], list[int | list]] = default_schema_gen,
            date: datetime | str = None,
            allow_random_count=True):

        self.generate_import(date=date)

        if allow_random_count:
            schemas_count = rnd.randint(0, schemas_count)

        for _ in range(schemas_count):
            self.generate_import(
                *schema_gen_func(),
                parent_id=rnd.choice(tuple(self._folder_ids)),
                is_new=False
            )

    def random_update(self):
        ids = set(self.ids) - self._imports[-1].items.keys()
        id_ = rnd.choice(tuple(ids))
        item = self._items[id_]

        allowed_parents = self._folder_ids.copy()

        if type(item) == Folder:
            allowed_parents -= item.children_ids | {id_}
            self.update_item(id_, parent_id=rnd.choice(tuple(allowed_parents)))
        else:
            updates = [
                ('parent_id', rnd.choice(tuple(allowed_parents))),
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
            i = rnd.choice(self.ids)
            return i, self.del_item(i)
        else:
            return None, None
