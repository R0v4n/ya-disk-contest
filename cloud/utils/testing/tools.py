from typing import Iterable

from deepdiff import DeepDiff
from devtools import debug
from sqlalchemy.engine import Connection

from cloud.api.model import NodeType
from cloud.db.schema import imports_table, folders_table, files_table, folder_history, file_history
from cloud.utils.testing import FakeCloud, get_node


def import_dataset(connection: Connection, dataset: dict):
    """Direct data insertion into db, without calculating folder sizes and history."""
    items = dataset['items']

    query = imports_table.insert().values({'date': dataset['updateDate']}).returning(imports_table.c.id)
    import_id = connection.execute(query).scalar()

    folders = [n for n in items if n['type'] == NodeType.FOLDER.value]
    files = [n for n in items if n['type'] == NodeType.FILE.value]

    def prepare_dicts(nodes: list[dict]):
        for n in nodes:
            t = n.pop('type')
            if 'parentId' in n:
                n['parent_id'] = n.pop('parentId')
            if t == NodeType.FOLDER.value:
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


def get_history_records(connection: Connection, type_: NodeType):
    """get all records from history table"""
    table = {NodeType.FOLDER: folder_history, NodeType.FILE: file_history}
    return [dict(row) for row in connection.execute(table[type_].select())]


def get_node_records(connection: Connection, type_: NodeType):
    """get all records from node table"""
    table = {NodeType.FOLDER: folders_table, NodeType.FILE: files_table}
    return [dict(row) for row in connection.execute(table[type_].select())]


def get_imports_records(connection: Connection):
    """get all records from imports table"""
    return [dict(row) for row in connection.execute(imports_table.select())]


def compare(received, expected, obj_note=None, **kwargs):
    if not kwargs:
        kwargs = dict(ignore_order=True)

    diff = DeepDiff(received, expected, **kwargs)
    try:
        assert diff == {}
    except AssertionError:
        if obj_note:
            debug(obj_note)
        debug(received)
        debug(expected)
        debug(diff)
        raise


def compare_db_fc_state(connection: Connection, fake_cloud: FakeCloud):

    received_imports = get_imports_records(connection)
    expected_imports = fake_cloud.get_raw_db_imports_records()

    compare(received_imports, expected_imports, 'imports:', ignore_order=True)

    received_files = get_node_records(connection, NodeType.FILE)
    received_folders = get_node_records(connection, NodeType.FOLDER)
    expected_files, expected_folders = fake_cloud.get_raw_db_node_records()
    compare(received_files, expected_files, 'files:', ignore_order=True)

    compare(received_folders, expected_folders, 'folders:', ignore_order=True)

    received_file_history = get_history_records(connection, NodeType.FILE)
    received_folder_history = get_history_records(connection, NodeType.FOLDER)
    expected_file_history, expected_folder_history = fake_cloud.get_raw_db_history_records()

    compare(received_file_history, expected_file_history, 'file history:', ignore_order=True)

    compare(received_folder_history, received_folder_history, 'folder history:', ignore_order=True)


async def compare_db_fc_node_trees(api_client, fake_cloud: FakeCloud, ids: Iterable[str] = None):
    if ids is None:
        ids = fake_cloud.ids

    for node_id in ids:
        expected_tree = fake_cloud.get_tree(node_id, nullify_folder_sizes=True)
        received_tree = await get_node(api_client, node_id)

        diff = DeepDiff(received_tree, expected_tree, ignore_order=True)
        assert diff == {}

__all__ = (
    'import_dataset',
    'get_history_records',
    'get_node_records',
    'get_imports_records',
    'compare_db_fc_node_trees',
    'compare_db_fc_state',
)
