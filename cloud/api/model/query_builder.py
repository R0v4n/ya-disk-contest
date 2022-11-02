from abc import ABC
from datetime import datetime, timedelta
from typing import Iterable, Any

from sqlalchemy import Table, select, func, exists, literal_column, String

from cloud.api.model.data_classes import NodeType
from cloud.db.schema import files_table, folder_history, folders_table, file_history, imports_table


class QueryBase(ABC):
    table: Table
    history_table: Table
    node_type: NodeType

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

        if len(ids) == 1:
            if isinstance(ids, set):
                return table.c.id == ids.copy().pop()
            if isinstance(ids, list):
                return table.c.id == ids[0]

        return table.c.id.in_(ids)

    @classmethod
    def select(cls, ids: Iterable[str] | str, columns: list[str] | None = None):
        columns = cls._build_columns(cls.table, columns)

        # todo: test efficiency of values table over IN
        # str_ids = ', '.join(f"('{i}')" for i in ids)
        #
        # ids_cte = \
        #     select([column('id', String)]). \
        #     select_from(
        #         text(f'(VALUES {str_ids}) as t (id)')
        #     ).cte()
        #
        # query = files.select().where(files.c.id == ids_cte.c.id)

        # todo: check efficiency. (value cte, exists?)

        return select(columns).where(cls._ids_condition(cls.table, ids))

    # todo: move to other class
    @classmethod
    def select_join_date(cls, table: Table, condition=None, columns: list[str | None] | None = None):
        query = select(cls._build_columns(table, columns) + [imports_table.c.date]). \
            select_from(table.join(imports_table))
        if condition is not None:
            query = query.where(condition)

        return query

    @classmethod
    def select_node_with_date(cls, node_id: str, columns: list[str | None] | None = None):
        columns += [literal_column(f"'{cls.node_type.value}'", String).label('type')]
        return cls.select_join_date(cls.table, cls._ids_condition(cls.table, node_id), columns)

    @classmethod
    def insert(cls, values: list[dict[str, Any]]):
        return cls.table.insert().values(values)

    # todo: this is not used. check import_model
    @classmethod
    def update(cls, bind_params):
        return cls.table.update(). \
            where(cls.table.c.id == bind_params.pop('id')). \
            values(**bind_params)

    @classmethod
    def select_parents_with_size(cls, ids: Iterable[str], add: bool = True):
        """
        Select direct parent folders for nodes with given ids.
        Third column in select is a total size of children with given ids for each parent.
        """
        exists = cls.select(ids, ['parent_id', 'size']).alias()
        sign = 1 if add else -1

        parents = \
            select([
                folders_table.c.id,
                folders_table.c.parent_id,
                func.sum(sign * exists.c.size).label('size')
            ]). \
                select_from(
                folders_table.join(
                    exists,
                    folders_table.c.id == exists.c.parent_id
                )
            ). \
                group_by(folders_table.c.id, folders_table.c.parent_id)

        return parents

    @classmethod
    def select_parents(cls, ids):
        """select strict parents all fields. May contain duplicate records!!!"""
        exists = cls.select(ids, ['parent_id']).alias()

        parents = \
            select(folders_table.columns). \
                select_from(
                folders_table.join(
                    exists,
                    folders_table.c.id == exists.c.parent_id
                )
            )

        return parents

    @classmethod
    def insert_history_from_select(cls, select_q):
        return cls.history_table.insert().from_select(cls.history_table.columns, select_q)

    @classmethod
    def exist(cls, node_id: str):
        return select([exists().where(cls.table.c.id == node_id)])

    @classmethod
    def delete(cls, node_id: str):
        return cls.table.delete().where(cls.table.c.id == node_id)

    @classmethod
    def recursive_parents(cls, node_id: str, columns: list[str] = None):

        node_select = cls.select(node_id, ['parent_id']).alias()
        first_parent_cte = select(cls._build_columns(folders_table, columns)). \
            select_from(folders_table). \
            where(folders_table.c.id == node_select). \
            cte(recursive=True)

        folders_alias = folders_table.alias()
        included_alias = first_parent_cte.alias()

        parent_folders = first_parent_cte.union_all(
            select(cls._build_columns(folders_alias, columns)).
            where(folders_alias.c.id == included_alias.c.parent_id)
        ).select()

        return parent_folders

    @classmethod
    def subtract_parents_size(cls, node_id: str):
        size = cls.select(node_id, ['size'])

        select_q = cls.recursive_parents(node_id).alias()

        query = folders_table.update(). \
            where(folders_table.c.id == select_q.c.id). \
            values(size=folders_table.c.size - size)

        return query

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

        # todo: refactor max(import_id)? after adding import_date validation or add indexing on import_date
        q2 = select([q.c.id, func.max(q.c.date).label('date')]).group_by(q.c.id).alias()

        q3 = select(q.columns).select_from(q.join(q2, (q.c.id == q2.c.id) & (q.c.date == q2.c.date))).distinct()
        return q3


class FolderQuery(QueryBase):
    table = folders_table
    history_table = folder_history
    node_type = NodeType.FOLDER

    def __init__(self):
        raise NotImplementedError('This class is a functor. No need to create instance.')

    # @classmethod
    # def recursive_children2(cls, folder_id: str):
    #     folder_cols: list[str | None] = ['id', 'parent_id', 'size']
    #     top_folder = cls.select_node_with_date(
    #         folder_id,
    #         folder_cols + [text('url')]
    #     ).cte(recursive=True)
    #     # print(top_folder)
    #     # exit()
    #     top_folder_alias = top_folder.alias()
    #     files_alias = files.alias()
    #     folders_alias = cls.table.alias()
    #
    #     s1 = cls.select_join_date(folders_alias, columns=folder_cols + [None])
    #     print(s1)
    #     s2 = cls.select_join_date(files_alias, columns=folder_cols + ['url'])
    #     print(s2)
    #     print()
    #     s3 = s1.union_all(s2).alias()
    #     print(select(s3.columns))
    #     print()
    #     children = top_folder.union_all(
    #         select(s3.columns).
    #         select_from(s3.join(
    #             top_folder_alias,
    #             s3.c.parent_id == top_folder_alias.c.id
    #         ))
    #     )
    #     return children.select()

    @classmethod
    def recursive_children(cls, folder_id: str):
        # todo: cols gets additional column 'type' in select_node_with_date. It's handles the task, but need refactor.
        cols = ['id', 'parent_id', 'size']

        top_folder = cls.select_node_with_date(
            folder_id,
            cols
        ).cte(recursive=True)

        folders_alias = cls.select_join_date(folders_table, columns=cols).alias()

        children = top_folder.union_all(
            select(folders_alias.columns).
            select_from(folders_alias.join(
                top_folder,
                folders_alias.c.parent_id == top_folder.c.id
            ))
        ).select()

        return children


class FileQuery(QueryBase):
    table = files_table
    history_table = file_history
    node_type = NodeType.FILE

    def __init__(self):
        raise NotImplementedError('This class is a functor. No need to create instance.')


class ImportQuery:

    def __init__(self, file_ids, folder_ids, import_id):
        self.import_id = import_id
        self.file_ids = file_ids
        self.folder_ids = folder_ids

    @classmethod
    def insert_import(cls, date: datetime):
        return imports_table.insert().values({'date': date}).returning(imports_table.c.id)

    def recursive_parents_with_size(self, add: bool = True):
        direct_parents = \
            FileQuery.select_parents_with_size(self.file_ids, add). \
                union_all(FolderQuery.select_parents_with_size(self.folder_ids, add)).alias()

        parents_cte = select([
            direct_parents.c.id,
            direct_parents.c.parent_id,
            func.sum(direct_parents.c.size).label('size')
        ]). \
            select_from(direct_parents). \
            group_by(direct_parents.c.id, direct_parents.c.parent_id).cte(recursive=True)

        folders_alias = folders_table.alias()
        parents_alias = parents_cte.alias()

        join_condition = (folders_alias.c.id == parents_alias.c.parent_id)
        if not add:
            # if two nodes (one child of another) was moved from one branch.
            join_condition &= ~ parents_alias.c.id.in_(self.folder_ids)

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

    def update_folder_sizes(self, add: bool = True):
        select_q = self.recursive_parents_with_size(add).alias()

        query = folders_table.update().where(folders_table.c.id == select_q.c.id).values(
            size=select_q.c.size + folders_table.c.size, import_id=self.import_id)

        return query

    def recursive_parents(self, ids):
        # todo: should empty folder addition considered an update for parent folders? what if folder delta_size==0?
        """
        select all folders that will be updated during import:
            1) new nodes parents
            2) current parents for updating nodes
            3) new parents for updating nodes
            4) existing folders in import
        """
        files_parents = FileQuery.select_parents(self.file_ids)

        parents_cte = folders_table. \
            select(). \
            where(folders_table.c.id.in_(ids)). \
            union(files_parents).alias().select().cte(recursive=True)

        folders_alias = folders_table.alias()
        parents_alias = parents_cte.alias()

        # todo: move cte to separate method?
        parent_folders = parents_cte.union(
            select(folders_alias.columns).where(folders_alias.c.id == parents_alias.c.parent_id)
        ).select()

        return parent_folders


class NodeQuery:
    def __init__(self, node_id):
        self.node_id = node_id

    def folder_children(self):
        return FolderQuery.recursive_children(self.node_id)

    def file_children(self):
        folders_select = FolderQuery.recursive_children(self.node_id).alias()

        cols = ['id', 'parent_id', 'size', 'url']
        cols += [literal_column(f"'{NodeType.FILE.value}'", String).label('type')]

        files_with_date = FileQuery.select_join_date(files_table, columns=cols).alias()
        query = select(files_with_date.columns). \
            select_from(files_with_date.join(folders_select))

        return query


if __name__ == '__main__':
    # print(FileQuery.select_node_with_date('1', ['id', 'parent_id', 'url', 'size']))
    # print(FolderQuery.recursive_children('1'))
    # print(NodeQuery('1').file_children())
    print(ImportQuery(['1', '11'], ['2', '3'], 1).update_folder_sizes(False))
    exit()
    # print(FolderQuery.subtract_parents_size('1'))
    ds = datetime.fromisoformat('2022-02-01 12:00:00 +00:00')
    de = datetime.fromisoformat('2022-02-02 12:00:00 +00:00')
    # print(de - timedelta(1))
    print(FileQuery.select_updates_daterange(ds, de))
