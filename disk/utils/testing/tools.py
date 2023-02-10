from dataclasses import dataclass
from typing import Iterable

from deepdiff import DeepDiff
from sqlalchemy.engine import Connection
from disk.db.schema import (
    imports_table, folders_table, files_table,
    folder_history, file_history, ItemType
)
from .fake_cloud import FakeCloud

__all__ = (
    'Dataset',
    'direct_import_to_db',
    'get_history_records',
    'get_node_records',
    'get_imports_records',
    'compare_db_fc_state',
    'compare'
)


def direct_import_to_db(connection: Connection, data: dict):
    """Direct data insertion into db, without calculating folder sizes and history."""
    items = data['items']

    query = imports_table.insert().values({'date': data['updateDate']}).returning(imports_table.c.id)
    import_id = connection.execute(query).scalar()

    folders = [n for n in items if n['type'] == ItemType.FOLDER.value]
    files = [n for n in items if n['type'] == ItemType.FILE.value]

    def prepare_dicts(nodes: list[dict]):
        for n in nodes:
            t = n.pop('type')
            if 'parentId' in n:
                n['parent_id'] = n.pop('parentId')
            if t == ItemType.FOLDER.value:
                if 'url' in n:
                    n.pop('url')
                n['size'] = 0

            n['import_id'] = import_id

    if folders:
        prepare_dicts(folders)
        connection.execute(folders_table.insert(), folders)

    if files:
        prepare_dicts(files)
        connection.execute(files_table.insert(), files)

    return import_id


def get_history_records(connection: Connection, type_: ItemType, ids: Iterable[str] | None = None):
    """get all records from history table"""
    if type_ == ItemType.FOLDER:
        table = folder_history
        col = folder_history.c.folder_id
    else:
        table = file_history
        col = file_history.c.file_id

    query = table.select()
    if ids:
        query = query.where(col.in_(list(ids)))

    return [dict(row) for row in connection.execute(query)]


def get_node_records(connection: Connection, type_: ItemType, ids: Iterable[str] | None = None):
    """get all records from node table"""
    table = {ItemType.FOLDER: folders_table, ItemType.FILE: files_table}
    query = table[type_].select()
    if ids:
        query = query.where(table[type_].c.id.in_(list(ids)))
    return [dict(row) for row in connection.execute(query)]


def get_imports_records(connection: Connection, ids: Iterable[int] | None = None):
    """get all records from imports table"""
    query = imports_table.select()
    if ids:
        query = query.where(imports_table.c.id.in_(list(ids)))
    return [dict(row) for row in connection.execute(query)]


def compare(
        received,
        expected,
        assertion_error_note=None,
        ignore_order=True,
        report_repetition=True,
        **kwargs):
    diff = DeepDiff(received, expected, ignore_order=ignore_order,
                    report_repetition=report_repetition, verbose_level=1, **kwargs)

    assert diff == {}, assertion_error_note


def compare_db_fc_state(connection: Connection, fake_cloud: FakeCloud):
    received_imports = get_imports_records(connection)
    expected_imports = fake_cloud.get_raw_db_imports_records()

    compare(received_imports, expected_imports, 'imports!',
            # note: imports table receive id from queue table, and it may distinguish from actual imports order.
            exclude_regex_paths=r"root\[\d+\]\['id'\]")

    received_files = get_node_records(connection, ItemType.FILE)
    received_folders = get_node_records(connection, ItemType.FOLDER)
    expected_files, expected_folders = fake_cloud.get_raw_db_node_records()

    compare(received_files, expected_files, 'files!',
            exclude_regex_paths=r"root\[\d+\]\['import_id'\]")
    compare(received_folders, expected_folders, 'folders!',
            exclude_regex_paths=r"root\[\d+\]\['import_id'\]")

    received_file_history = get_history_records(connection, ItemType.FILE)
    received_folder_history = get_history_records(connection, ItemType.FOLDER)
    expected_file_history, expected_folder_history = fake_cloud.get_raw_db_history_records()
    compare(received_file_history, expected_file_history, 'file history!',
            exclude_regex_paths=r"root\[\d+\]\['import_id'\]")
    compare(received_folder_history, expected_folder_history, 'folder history!',
            exclude_regex_paths=r"root\[\d+\]\['import_id'\]")


@dataclass
class Dataset:
    """Namespace for convenience"""
    import_dict: dict | None = None
    node_id: str | None = None
    expected_tree: dict | None = None
    expected_history: list[dict] | None = None
