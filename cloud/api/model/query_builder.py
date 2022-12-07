from abc import ABC
from datetime import datetime
from typing import Iterable, Any

from sqlalchemy import Table, select, func, exists, literal_column, String, union_all
from sqlalchemy.sql.elements import Null

from cloud.db.schema import files_table, folder_history, folders_table, file_history, imports_table
from .data_classes import ItemType


def insert_import_query(date: datetime):
    return imports_table.insert().values({'date': date}).returning(imports_table.c.id)


class CommonQueryMixin:

    @staticmethod
    def _build_columns(table: Table, columns: list[str | None] | None = None):

        if columns is None:
            return table.columns
        else:
            return [table.c[name] if isinstance(name, str) else name for name in columns]

    @staticmethod
    def _ids_condition(table: Table, ids: list[str] | set[str] | str):
        if type(ids) == str:
            return table.c.id == ids

        # todo: does it make sense?
        if len(ids) == 1:
            if isinstance(ids, set):
                return table.c.id == ids.copy().pop()
            if isinstance(ids, list):
                return table.c.id == ids[0]

        return table.c.id.in_(ids)

    @classmethod
    def select_join_date(cls, table: Table, condition=None, columns: list[str | None] | None = None):
        query = select(cls._build_columns(table, columns) + [imports_table.c.date]). \
            select_from(table.join(imports_table))
        if condition is not None:
            query = query.where(condition)

        return query


class QueryBase(ABC, CommonQueryMixin):
    """Base functor class for queries"""

    table: Table
    history_table: Table
    node_type: ItemType

    @classmethod
    def select(cls, ids: Iterable[str] | str, columns: list[str] | None = None):
        columns = cls._build_columns(cls.table, columns)
        return select(columns).where(cls._ids_condition(cls.table, ids))

    @classmethod
    def select_node_with_date(cls, node_id: str, columns: list[str | None] | None = None):
        columns += [literal_column(f"'{cls.node_type.value}'", String).label('type')]
        return cls.select_join_date(cls.table, cls._ids_condition(cls.table, node_id), columns)

    @classmethod
    def insert(cls, values: list[dict[str, Any]]):
        return cls.table.insert().values(values)

    # note: this is not used. check import_model
    @classmethod
    def update(cls, bind_params):
        return cls.table.update(). \
            where(cls.table.c.id == bind_params.pop('id')). \
            values(**bind_params)

    @classmethod
    def direct_parents(cls, ids, columns: list[str | None] | None = None):
        """select direct parents. May contain duplicate records!"""

        folders_alias = folders_table.alias()

        parents = \
            select(cls._build_columns(folders_alias, columns)). \
            select_from(
                folders_alias.join(
                    cls.table,
                    cls.table.c.parent_id == folders_alias.c.id
                )
            ).where(cls._ids_condition(cls.table, ids))

        return parents

    @classmethod
    def insert_history_from_select(cls, select_q):
        return cls.history_table.insert().from_select(cls.history_table.columns, select_q)

    @classmethod
    def exist(cls, ids: str | Iterable[str]):
        return select([exists().where(cls._ids_condition(cls.table, ids))])

    @classmethod
    def delete(cls, node_id: str):
        return cls.table.delete().where(cls.table.c.id == node_id)

    @classmethod
    def recursive_parents(cls, ids: str | Iterable[str], columns: list[str] = None):
        direct_parents = cls.direct_parents(ids, columns).cte(recursive=True)

        folders_alias = folders_table.alias()
        included_alias = direct_parents.alias()

        parent_folders = direct_parents.union_all(
            select(cls._build_columns(folders_alias, columns)).
            where(folders_alias.c.id == included_alias.c.parent_id)
        ).select()

        return parent_folders

    @classmethod
    def select_nodes_union_history_in_daterange(cls, date_start: datetime, date_end: datetime,
                                                ids: Iterable[str] | str = None, closed=True):
        union_cte = cls.table.select().union_all(cls.history_table.select()).cte()

        cols = set(union_cte.columns) - {union_cte.c.import_id}

        condition = (imports_table.c.date <= date_end) if closed else (imports_table.c.date < date_end)
        condition &= (date_start <= imports_table.c.date)
        if ids:
            condition &= cls._ids_condition(union_cte, ids)

        query = cls.select_join_date(union_cte, condition, cols)

        return query

    @classmethod
    def select_updates_daterange(cls, date_start: datetime, date_end: datetime,
                                 ids: Iterable[str] | str = None, closed=True):
        q = cls.select_nodes_union_history_in_daterange(date_start, date_end, ids, closed).cte()

        q2 = select([q.c.id, func.max(q.c.date).label('date')]).group_by(q.c.id).alias()

        q3 = select(q.columns).select_from(q.join(q2, (q.c.id == q2.c.id) & (q.c.date == q2.c.date))).distinct()
        return q3


class FolderQuery(QueryBase):
    """Functor class for folder_table queries"""

    table = folders_table
    history_table = folder_history
    node_type = ItemType.FOLDER

    @classmethod
    def folder_tree_cte(cls, folder_id: str):
        cols = ['id', 'parent_id', 'size', Null().label('url'), imports_table.c.date]

        # cols gets additional column 'type' in select_node_with_date. It's handles the task, but need refactor.
        top_folder = cls.select_node_with_date(
            folder_id,
            cols
        ).cte(recursive=True)

        cte = top_folder.union_all(
            select(cls._build_columns(cls.table, cols)).
            select_from(
                cls.table.join(imports_table).join(
                    top_folder,
                    cls.table.c.parent_id == top_folder.c.id
                )
            )
        )

        return cte

    @classmethod
    def select_folder_tree(cls, folder_id):
        tree_cte = cls.folder_tree_cte(folder_id)

        cols = ['id', 'parent_id', 'size', 'url', imports_table.c.date]
        cols += [literal_column(f"'{ItemType.FILE.value}'", String).label('type')]

        query = select(cls._build_columns(files_table, cols)). \
            select_from(
            files_table.join(imports_table).
            join(tree_cte)
        ).union_all(tree_cte.select())

        return query

    @classmethod
    def recursive_parents_with_size(cls, file_ids, folder_ids, add: bool = True):

        direct_parents = union_all(
            FileQuery.direct_parents(file_ids, ['id', 'parent_id', files_table.c.size]),
            cls.direct_parents(folder_ids, ['id', 'parent_id', folders_table.c.size])
        ).alias()

        sign = 1 if add else -1

        parents_cte = \
            select([
                direct_parents.c.id,
                direct_parents.c.parent_id,
                func.sum(sign*direct_parents.c.size).label('size')
            ]). \
            group_by(direct_parents.c.id, direct_parents.c.parent_id).cte(recursive=True)

        folders_alias = folders_table.alias()
        parents_alias = parents_cte.alias()

        join_condition = (folders_alias.c.id == parents_alias.c.parent_id)
        if not add:
            # if two nodes (one child of another) was moved from one branch.
            join_condition &= ~ parents_alias.c.id.in_(folder_ids)

        parents_recursive = parents_cte.union_all(
            select([
                folders_alias.c.id,
                folders_alias.c.parent_id,
                parents_alias.c.size
            ]).
            select_from(folders_alias.join(parents_alias, join_condition))
        )

        all_parents = \
            select(
                [parents_recursive.c.id,
                 func.sum(parents_recursive.c.size).label('size')]
            ). \
            select_from(parents_recursive). \
            group_by(parents_recursive.c.id)

        return all_parents

    @classmethod
    def update_parent_sizes(
            cls,
            file_ids: Iterable[str] | str,
            folder_ids: Iterable[str] | str,
            import_id: int,
            add: bool = True):

        select_q = cls.recursive_parents_with_size(file_ids, folder_ids, add).alias()

        query = folders_table.update().where(folders_table.c.id == select_q.c.id).values(
            size=select_q.c.size + folders_table.c.size, import_id=import_id)

        return query


class FileQuery(QueryBase):
    """Functor class for file_table queries"""

    table = files_table
    history_table = file_history
    node_type = ItemType.FILE


