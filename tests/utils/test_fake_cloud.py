from datetime import timedelta

import pytest
from pytest_cases import parametrize_with_cases, unpack_fixture

from cloud.model import ItemType
from cloud.utils.testing import FakeCloud, Dataset, compare
from tests import post_import_cases, get_node_cases


@parametrize_with_cases('dataset', post_import_cases)
def test_load_import_with_static_data(fake_cloud_module, dataset: Dataset):
    fake_cloud_module.load_import(dataset.import_dict)
    compare(dataset.import_dict, fake_cloud_module.get_import_dict())
    compare(fake_cloud_module.get_all_history(), dataset.expected_history)


@parametrize_with_cases('dataset', get_node_cases)
def test_node_tree_with_static_data(fake_cloud, dataset: Dataset):
    fake_cloud.load_import(dataset.import_dict)
    compare(dataset.import_dict, fake_cloud.get_import_dict())
    compare(
        fake_cloud.get_tree(dataset.node_id, nullify_folder_sizes=True),
        dataset.expected_tree
    )


@pytest.mark.parametrize('schemas',
                         [(1,), ([],), ([1],), ([1], 1), ([2, [2]], [1], 2, 1)],
                         ids=(lambda s: ', '.join(str(i) for i in s)))
def test_generate_with_load(schemas):
    fc1 = FakeCloud()
    fc2 = FakeCloud()

    fc1.generate_import(*schemas)
    data1 = fc1.get_import_dict()
    fc2.load_import(data1)

    compare(data1, fc2.get_import_dict())


cloud = FakeCloud()
cloud.generate_import([2, [1]], 1)


@pytest.mark.parametrize('item, type_, parent_id', [
    (0, ItemType.FOLDER.value, None),
    (1, ItemType.FILE.value, None),
    ((0, 0), ItemType.FILE.value, cloud[0].id),
    ((0, 1), ItemType.FILE.value, cloud[0].id),
    ((0, 2), ItemType.FOLDER.value, cloud[0].id),
    ((0, 2, 0), ItemType.FILE.value, cloud[0, 2].id)
])
def test_getitem(item, type_, parent_id):
    assert cloud[item].type == type_
    assert cloud[item].parent_id == parent_id


@pytest.mark.parametrize('item', [2, (0, 3), (1, 0), (0, 2, 1), (0, 2, 0, 0)])
def test_incorrect_getitem(item):
    with pytest.raises(IndexError):
        _ = cloud[item]


def test_generate_import_parent_size():
    d1 = cloud[0]
    assert d1.size == sum(cloud[0, i].size for i in range(3))


def test_delete(fake_cloud):
    fake_cloud.generate_import([[1]])

    file = fake_cloud[0, 0, 0]
    dir1 = fake_cloud[0]
    dir2 = fake_cloud[0, 0]

    date = fake_cloud.del_item(file.id)

    assert date > fake_cloud._imports[0].date

    assert fake_cloud.get_node_copy(dir1.id).size + file.size == dir1.size
    assert fake_cloud.get_node_copy(dir2.id).size + file.size == dir2.size
    assert fake_cloud.get_node_copy(dir1.id).date == date
    assert fake_cloud.get_node_copy(dir2.id).date == date

    assert len(fake_cloud._items) == 3
    with pytest.raises(KeyError):
        fake_cloud.get_node_copy(file.id)

    expected_history = [dir1.export_dict, dir2.export_dict]

    compare(
        fake_cloud.get_all_history(),
        expected_history
    )


@pytest.fixture
def prepare(fake_cloud):
    fake_cloud.generate_import(2)
    f1 = fake_cloud[0]
    f2 = fake_cloud[1]

    fake_cloud.generate_import(date=f1.date + timedelta(hours=1))
    fake_cloud.update_item(f1.id, url='ortega')

    return fake_cloud, f1.export_dict, f2.export_dict, fake_cloud.get_node_copy(f1.id).export_dict


unpack_fixture('filled_cloud, file1_old, file2_old, file1_new', prepare)


def date_end_minus_two(filled_cloud: FakeCloud):
    return (
        filled_cloud.last_import_date + timedelta(hours=-2),
        []
    )


def date_end_minus_one(filled_cloud: FakeCloud, file1_old, file2_old):
    return (
        filled_cloud.last_import_date + timedelta(hours=-1),
        [file1_old, file2_old]
    )


def date_end_between_imports(filled_cloud: FakeCloud, file1_old, file2_old):
    return (
        filled_cloud.last_import_date + timedelta(hours=-0.5),
        [file1_old, file2_old]
    )


def date_end_zero(filled_cloud: FakeCloud, file1_new, file2_old):
    return (
        filled_cloud.last_import_date,
        [file1_new, file2_old]
    )


def date_end_23(filled_cloud: FakeCloud, file1_new, file2_old):
    return (
        filled_cloud.last_import_date,
        [file1_new, file2_old]
    )


def date_end_23_5(filled_cloud: FakeCloud, file1_new):
    return (
        filled_cloud.last_import_date + timedelta(hours=23.5),
        [file1_new]
    )


def date_end_24(filled_cloud: FakeCloud, file1_new):
    return (
        filled_cloud.last_import_date + timedelta(hours=24),
        [file1_new]
    )


def date_end_25(filled_cloud: FakeCloud):
    return (
        filled_cloud.last_import_date + timedelta(hours=25),
        []
    )


@parametrize_with_cases('date, expected', '.', prefix='date_end_')
def test_updates(filled_cloud: FakeCloud, date, expected):
    compare(
        filled_cloud.get_updates(date_end=date),
        {'items': expected}
    )
