from datetime import datetime
from enum import IntEnum
from typing import Iterable, Any

from sqlalchemy import select, func

from cloud.db.schema import imports_table, queue_table, files_table, folders_table
from .tools import ids_condition, build_columns
from .item_table_queries import FileQuery, FolderQuery


def insert_import_auto_id(date: datetime):
    return imports_table.insert().values({'date': date}).returning(imports_table.c.id)


def insert_import(import_id: int, date: datetime):
    return imports_table.insert().values({'id': import_id, 'date': date})


def insert_queue(date: datetime):
    return queue_table.insert().values({'date': date}).returning(queue_table.c.id)


def get_oldest_queue_id():
    return select([queue_table.c.id]).order_by(queue_table.c.date).limit(1)


def delete_queue(id_: int):
    return queue_table.delete().where(queue_table.c.id == id_)


def lock_ids_from_select(cte):
    return select([func.pg_advisory_xact_lock(func.hashtextextended(cte.c.id, 0))])


class Sign(IntEnum):
    ADD = 1
    SUB = -1


def recursive_parents_with_size(file_ids: Iterable[str] | str | None,
                                folder_ids: Iterable[str] | str | None, sign: Sign = Sign.ADD):
    if file_ids:
        # in this case string cols are parents cols, so child sizes need to be explicit
        direct_parents = FileQuery.direct_parents(file_ids, ['id', 'parent_id', files_table.c.size])

        if folder_ids:
            direct_parents = direct_parents.union_all(
                FolderQuery.direct_parents(folder_ids, ['id', 'parent_id', folders_table.c.size])
            )

    elif folder_ids:
        direct_parents = FolderQuery.direct_parents(folder_ids, ['id', 'parent_id', folders_table.c.size])

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
        join_condition &= ~ ids_condition(parents_alias, folder_ids)

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


def update_parent_sizes(
        file_ids: Iterable[str] | str | None,
        folder_ids: Iterable[str] | str | None,
        import_id: int,
        sign: Sign = Sign.ADD):

    select_q = recursive_parents_with_size(file_ids, folder_ids, sign).alias()

    query = folders_table.update().where(folders_table.c.id == select_q.c.id).values(
        size=select_q.c.size + folders_table.c.size, import_id=import_id)

    return query


def folders_with_recursive_parents_cte(
        folder_ids: Iterable[str] | str | None,
        child_file_ids: Iterable[str] | str | None,
        columns: list[str | Any] = None
):
    """
    Select folders records:
        - with ids belongs to folder_ids,
        - direct parents of files with ids belongs to child_file_ids
        - and all recursive parents of folders mentioned above
    """
    if folder_ids:
        direct_parents = FolderQuery.select(folder_ids, columns)
        if child_file_ids:
            files_parents = FileQuery.direct_parents(child_file_ids, columns)
            direct_parents = files_parents.union_all(direct_parents)

    elif child_file_ids:
        direct_parents = FileQuery.direct_parents(child_file_ids, columns)
    else:
        raise ValueError('file_ids or folder_ids should be non empty')

    direct_parents = direct_parents.alias().select().distinct().cte(recursive=True)

    folders_alias = folders_table.alias()
    included_alias = direct_parents.alias()

    parent_folders = direct_parents.union(
        select(build_columns(folders_alias, columns)).
        where(folders_alias.c.id == included_alias.c.parent_id)
    )

    return parent_folders
