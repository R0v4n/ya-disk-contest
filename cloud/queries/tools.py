from typing import Iterable, Any

from sqlalchemy import Table


def build_columns(table: Table, columns: Iterable[str | Any] | None = None):

    if columns is None:
        return table.columns
    else:
        return [table.c[name] if isinstance(name, str) else name for name in columns]


def ids_condition(table: Table, ids: Iterable[str] | str):
    if type(ids) == str:
        return table.c.id == ids

    return table.c.id.in_(ids)
