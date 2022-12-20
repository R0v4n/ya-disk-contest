from dataclasses import dataclass
from typing import Iterable

from deepdiff import DeepDiff
from sqlalchemy.engine import Connection

from cloud.api.model import ItemType
from cloud.db.schema import imports_table, folders_table, files_table, folder_history, file_history
from .api_methods import get_node
from .fake_cloud import FakeCloud

__all__ = (
    'Dataset',
    'direct_import_to_db',
    'get_history_records',
    'get_node_records',
    'get_imports_records',
    'compare_db_fc_node_trees',
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


def get_history_records(connection: Connection, type_: ItemType):
    """get all records from history table"""
    table = {ItemType.FOLDER: folder_history, ItemType.FILE: file_history}
    return [dict(row) for row in connection.execute(table[type_].select())]


def get_node_records(connection: Connection, type_: ItemType):
    """get all records from node table"""
    table = {ItemType.FOLDER: folders_table, ItemType.FILE: files_table}
    return [dict(row) for row in connection.execute(table[type_].select())]


def get_imports_records(connection: Connection):
    """get all records from imports table"""
    return [dict(row) for row in connection.execute(imports_table.select())]


def compare(
        received,
        expected,
        assertion_error_note=None,
        ignore_order=True,
        report_repetition=True,
        **kwargs):

    diff = DeepDiff(received, expected, ignore_order=ignore_order, report_repetition=report_repetition, **kwargs)
    assert diff == {}, assertion_error_note


def compare_db_fc_state(connection: Connection, fake_cloud: FakeCloud):

    received_imports = get_imports_records(connection)
    expected_imports = fake_cloud.get_raw_db_imports_records()

    compare(received_imports, expected_imports, 'imports!')

    received_files = get_node_records(connection, ItemType.FILE)
    received_folders = get_node_records(connection, ItemType.FOLDER)
    expected_files, expected_folders = fake_cloud.get_raw_db_node_records()

    compare(received_files, expected_files, 'files!')
    compare(received_folders, expected_folders, 'folders!')

    received_file_history = get_history_records(connection, ItemType.FILE)
    received_folder_history = get_history_records(connection, ItemType.FOLDER)
    expected_file_history, expected_folder_history = fake_cloud.get_raw_db_history_records()

    compare(received_file_history, expected_file_history, 'file history!')
    compare(received_folder_history, expected_folder_history, 'folder history!')


async def compare_db_fc_node_trees(api_client, fake_cloud: FakeCloud,
                                   ids: Iterable[str] = None, nullify_folder_sizes=True):
    if ids is None:
        ids = fake_cloud.ids

    for node_id in ids:
        expected_tree = fake_cloud.get_tree(node_id, nullify_folder_sizes=nullify_folder_sizes)
        received_tree = await get_node(api_client, node_id)

        compare(received_tree, expected_tree)


@dataclass
class Dataset:
    """Namespace for convenience"""
    import_dict: dict | None = None
    node_id: str | None = None
    expected_tree: dict | None = None
    expected_history: list[dict] | None = None
