from abc import ABC, abstractmethod
from datetime import datetime
from enum import IntEnum
from typing import Iterable, Any, TypeVar

from sqlalchemy import Table, select, func, exists, literal_column, String
from sqlalchemy.sql.elements import Null

from cloud.db.schema import files_table, folder_history, folders_table, file_history, imports_table, queue_table
from .schemas import ItemType


class Sign(IntEnum):
    ADD = 1
    SUB = -1


def insert_import_query(date: datetime):
    return imports_table.insert().values({'date': date}).returning(imports_table.c.id)


def insert_import_from_mdl_query(import_id: int, date: datetime):
    return imports_table.insert().values({'id': import_id, 'date': date})


def insert_queue_query(date: datetime):
    return queue_table.insert().values({'date': date}).returning(queue_table.c.id)


def get_oldest_queue_id():
    return select([queue_table.c.id]).order_by(queue_table.c.date).limit(1)


def delete_queue(id_: int):
    return queue_table.delete().where(queue_table.c.id == id_)


class QueryToolsMixin:

    @staticmethod
    def _build_columns(table: Table, columns: Iterable[str | Any] | None = None):

        if columns is None:
            return table.columns
        else:
            return [table.c[name] if isinstance(name, str) else name for name in columns]

    @staticmethod
    def _ids_condition(table: Table, ids: Iterable[str] | str):
        if type(ids) == str:
            return table.c.id == ids

        return table.c.id.in_(ids)


class QueryBase(ABC, QueryToolsMixin):
    """Base class for queries"""

    table: Table
    history_table: Table
    node_type: ItemType

    @classmethod
    def select(cls, ids: Iterable[str] | str, columns: list[str] | None = None):
        columns = cls._build_columns(cls.table, columns)
        return select(columns).where(cls._ids_condition(cls.table, ids))

    @classmethod
    def select_node_with_date(cls, node_id: str, columns: list[str] | None = None):
        """
        Select node record with additional field date.
        """
        return select(cls._build_columns(cls.table, columns) + [imports_table.c.date]). \
            select_from(cls.table.join(imports_table)). \
            where(cls.table.c.id == node_id)

    @classmethod
    def insert(cls, values: list[dict[str, Any]]):
        return cls.table.insert().values(values)

    @classmethod
    def update_many(cls, mapping: dict[str, str]):
        id_param = mapping.pop('id')

        cols = ', '.join(f'{key}={val}' for key, val in mapping.items())

        return f'UPDATE {cls.table.name} SET {cols} WHERE id = {id_param}'

    @classmethod
    def direct_parents(cls, ids: Iterable[str] | str, columns: list[str | None] | None = None):
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

        return select(cls._build_columns(union_cte, cols) + [imports_table.c.date]). \
            select_from(union_cte.join(imports_table)). \
            where(condition)

    @classmethod
    def select_updates_daterange(cls, date_start: datetime, date_end: datetime,
                                 ids: Iterable[str] | str = None, closed=True):
        q = cls.select_nodes_union_history_in_daterange(date_start, date_end, ids, closed).cte()

        q2 = select([q.c.id, func.max(q.c.date).label('date')]).group_by(q.c.id).alias()

        q3 = select(q.columns).select_from(q.join(q2, (q.c.id == q2.c.id) & (q.c.date == q2.c.date))).distinct()
        return q3

    @classmethod
    @abstractmethod
    def get_node_select_query(cls, node_id: str):
        """:return: select query for get node API method"""


QueryT = TypeVar('QueryT', bound=QueryBase)


class FolderQuery(QueryBase):
    """Class for folder_table queries"""

    table = folders_table
    history_table = folder_history
    node_type = ItemType.FOLDER

    @classmethod
    def folder_tree_cte(cls, folder_id: str):
        cols = ['id', 'parent_id', 'size', Null().label('url'), imports_table.c.date]

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
    def select_folder_tree(cls, folder_id: str):
        tree_cte = cls.folder_tree_cte(folder_id)

        file_cols = ['id', 'parent_id', 'size', 'url', imports_table.c.date,
                     literal_column(f"'{ItemType.FILE.value}'", String).label('type')]

        folder_cols = tree_cte.c + [literal_column(f"'{ItemType.FOLDER.value}'", String).label('type')]

        query = select(folder_cols). \
            select_from(tree_cte). \
            union_all(
            select(cls._build_columns(files_table, file_cols)).
            select_from(files_table.join(imports_table).join(tree_cte))
        )

        return query

    @classmethod
    def get_node_select_query(cls, node_id: str):
        return cls.select_folder_tree(node_id)

    @classmethod
    def recursive_parents_with_size(cls, file_ids: Iterable[str] | str | None,
                                    folder_ids: Iterable[str] | str | None, sign: Sign = Sign.ADD):
        if file_ids:
            direct_parents = FileQuery.direct_parents(file_ids, ['id', 'parent_id', files_table.c.size])

            if folder_ids:
                direct_parents = direct_parents.union_all(
                    cls.direct_parents(folder_ids, ['id', 'parent_id', folders_table.c.size])
                )

        elif folder_ids:
            direct_parents = cls.direct_parents(folder_ids, ['id', 'parent_id', folders_table.c.size])

        else:
            raise ValueError('file_ids or folder_ids should exists')

        direct_parents = direct_parents.alias()

        parents_cte = \
            select([
                direct_parents.c.id,
                direct_parents.c.parent_id,
                func.sum(sign * direct_parents.c.size).label('size')
            ]). \
                group_by(direct_parents.c.id, direct_parents.c.parent_id).cte(recursive=True)

        folders_alias = folders_table.alias()
        parents_alias = parents_cte.alias()

        join_condition = (folders_alias.c.id == parents_alias.c.parent_id)
        if sign == Sign.SUB and folder_ids:
            # if two nodes (one child of another) was moved from one branch.
            join_condition &= ~ cls._ids_condition(parents_alias, folder_ids)

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
            file_ids: Iterable[str] | str | None,
            folder_ids: Iterable[str] | str | None,
            import_id: int,
            sign: Sign = Sign.ADD):

        select_q = cls.recursive_parents_with_size(file_ids, folder_ids, sign).alias()

        query = folders_table.update().where(folders_table.c.id == select_q.c.id).values(
            size=select_q.c.size + folders_table.c.size, import_id=import_id)

        return query

    @classmethod
    def insert_history(cls, ids):
        return f"""WITH RECURSIVE anon_1(import_id, id, parent_id, size) AS
                   (SELECT folders.import_id AS import_id,
                           folders.id        AS id,
                           folders.parent_id AS parent_id,
                           folders.size      AS size
                    FROM folders
                    WHERE folders.id IN ({', '.join(f"'{i}'" for i in ids)})
                    UNION ALL
                    SELECT folders_1.import_id AS import_id,
                           folders_1.id        AS id,
                           folders_1.parent_id AS parent_id,
                           folders_1.size      AS size
                    FROM folders AS folders_1,
                         anon_1 AS anon_2
                    WHERE folders_1.id = anon_2.parent_id)
                    INSERT
                    INTO folder_history (import_id, folder_id, parent_id, size)
                    SELECT import_id, id, parent_id, size
                    FROM (SELECT anon_1.import_id,
                                 anon_1.id,
                                 anon_1.parent_id,
                                 anon_1.size,
                                 pg_advisory_xact_lock(hashtextextended(anon_1.id, 0))
                          FROM anon_1) as tmp"""

    @classmethod
    def lock_rows(cls, ids):
        return f"""WITH RECURSIVE anon_1(import_id, id, parent_id, size) AS
                   (SELECT folders.import_id AS import_id,
                           folders.id        AS id,
                           folders.parent_id AS parent_id,
                           folders.size      AS size
                    FROM folders
                    WHERE folders.id IN ({', '.join(f"'{i}'" for i in ids)})
                    UNION ALL
                    SELECT folders_1.import_id AS import_id,
                           folders_1.id        AS id,
                           folders_1.parent_id AS parent_id,
                           folders_1.size      AS size
                    FROM folders AS folders_1,
                         anon_1 AS anon_2
                    WHERE folders_1.id = anon_2.parent_id)
SELECT pg_advisory_xact_lock(hashtextextended(anon_1.id, 0)) FROM anon_1"""


class FileQuery(QueryBase):
    """Class for file_table queries"""

    table = files_table
    history_table = file_history
    node_type = ItemType.FILE

    @classmethod
    def get_node_select_query(cls, node_id: str):
        columns = ['id', 'parent_id', 'url', 'size',
                   literal_column(f"'{cls.node_type.value}'", String).label('type')]
        return cls.select_node_with_date(node_id, columns)
