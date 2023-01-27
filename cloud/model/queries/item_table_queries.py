from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Any, TypeVar

from sqlalchemy import Table, select, func, exists, literal_column, String
from sqlalchemy.sql.elements import Null

from cloud.db.schema import files_table, folder_history, folders_table, file_history, imports_table
from cloud.model.schemas import ItemType
from .tools import build_columns, ids_condition


class ItemQueryBase(ABC):
    """Base class for queries"""

    table: Table
    history_table: Table
    node_type: ItemType

    @classmethod
    def select(cls, ids: Iterable[str] | str, columns: list[str] | None = None):
        columns = build_columns(cls.table, columns)
        return select(columns).where(ids_condition(cls.table, ids))

    @classmethod
    def select_node_with_date(cls, node_id: str, columns: list[str] | None = None):
        """
        Select node record with additional field date.
        """
        return select(build_columns(cls.table, columns) + [imports_table.c.date]). \
            select_from(cls.table.join(imports_table)). \
            where(cls.table.c.id == node_id)

    @classmethod
    def insert(cls, values: list[dict[str, Any]]):
        return cls.table.insert().values(values)

    @classmethod
    def update_many(cls, mapping: dict[str, str]):
        id_param = mapping['id']

        cols = ', '.join(f'{key}={val}' for key, val in mapping.items()
                         if key != 'id')

        return f'UPDATE {cls.table.name} SET {cols} WHERE id = {id_param}'

    @classmethod
    def direct_parents(cls, ids: Iterable[str] | str, columns: list[str | None] | None = None):
        """
        Select direct parents. May contain duplicate records!
        It's necessary for collecting children sizes in FolderQuery.update_parent_sizes query.
        It's not anti-SOLID or whatever. Just a reminder in case direct parents without duplicates are needed.
        """

        folders_alias = folders_table.alias()

        parents = \
            select(build_columns(folders_alias, columns)). \
            select_from(
                folders_alias.join(
                    cls.table,
                    cls.table.c.parent_id == folders_alias.c.id
                )
            ).where(ids_condition(cls.table, ids))

        return parents

    @classmethod
    def insert_history_from_select(cls, select_q):
        return cls.history_table.insert().from_select(cls.history_table.columns, select_q)

    @classmethod
    def exist(cls, ids: str | Iterable[str]):
        return select([exists().where(ids_condition(cls.table, ids))])

    @classmethod
    def delete(cls, node_id: str):
        return cls.table.delete().where(cls.table.c.id == node_id)

    # note: this query can retrieve duplicates.
    #  For now it's not a problem, because it is used only for а single id
    #  and there can be no duplicates
    @classmethod
    def recursive_parents(cls, ids: str | Iterable[str], columns: list[str] = None):
        direct_parents = cls.direct_parents(ids, columns).cte(recursive=True)

        folders_alias = folders_table.alias()
        included_alias = direct_parents.alias()

        parent_folders = direct_parents.union_all(
            select(build_columns(folders_alias, columns)).
            where(folders_alias.c.id == included_alias.c.parent_id)
        )

        return parent_folders

    @classmethod
    def select_nodes_union_history_in_daterange(cls, date_start: datetime, date_end: datetime,
                                                ids: Iterable[str] | str = None, closed=True):
        union_cte = cls.table.select().union_all(cls.history_table.select()).cte()

        cols = set(union_cte.columns) - {union_cte.c.import_id}

        condition = (imports_table.c.date <= date_end) if closed else (imports_table.c.date < date_end)
        condition &= (date_start <= imports_table.c.date)
        if ids:
            condition &= ids_condition(union_cte, ids)

        return select(build_columns(union_cte, cols) + [imports_table.c.date]). \
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

    @classmethod
    def xact_advisory_lock_parent_ids(cls, ids: str | Iterable[str]):
        cte = cls.recursive_parents(ids)
        return select([func.pg_advisory_xact_lock(func.hashtextextended(cte.c.id, 0))])


QueryT = TypeVar('QueryT', bound=ItemQueryBase)


class FolderQuery(ItemQueryBase):
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
            select(build_columns(cls.table, cols)).
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
            select(build_columns(files_table, file_cols)).
            select_from(files_table.join(imports_table).join(tree_cte))
        )

        return query

    @classmethod
    def get_node_select_query(cls, node_id: str):
        return cls.select_folder_tree(node_id)


class FileQuery(ItemQueryBase):
    """Class for file_table queries"""

    table = files_table
    history_table = file_history
    node_type = ItemType.FILE

    @classmethod
    def get_node_select_query(cls, node_id: str):
        columns = ['id', 'parent_id', 'url', 'size',
                   literal_column(f"'{cls.node_type.value}'", String).label('type')]
        return cls.select_node_with_date(node_id, columns)
