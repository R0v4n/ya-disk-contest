import pytest
from deepdiff import DeepDiff
from pytest_cases import parametrize_with_cases

from cloud.api.model import ItemType
from cloud.utils.testing import FakeCloud, Dataset
from tests import post_import_cases, get_node_cases


@parametrize_with_cases('dataset', post_import_cases)
def test_load_import_with_static_data(fake_cloud_module, dataset: Dataset):
    fake_cloud_module.load_import(dataset.import_dict)
    assert DeepDiff(dataset.import_dict, fake_cloud_module.get_import_dict(), ignore_order=True) == {}
    assert DeepDiff(fake_cloud_module.get_all_history(), dataset.expected_history, ignore_order=True) == {}


@parametrize_with_cases('dataset', get_node_cases)
def test_node_tree_with_static_data(fake_cloud, dataset: Dataset):
    fake_cloud.load_import(dataset.import_dict)
    assert DeepDiff(dataset.import_dict, fake_cloud.get_import_dict(), ignore_order=True) == {}
    assert DeepDiff(
        fake_cloud.get_tree(dataset.node_id, nullify_folder_sizes=True),
        dataset.expected_tree, ignore_order=True
    ) == {}


@pytest.mark.parametrize('schemas',
                         [(1, ), ([],), ([1],), ([1], 1), ([2, [2]], [1], 2, 1)],
                         ids=(lambda s: ', '.join(str(i) for i in s)))
def test_generate_with_load(schemas):
    fc1 = FakeCloud()
    fc2 = FakeCloud()

    fc1.generate_import(*schemas)
    data1 = fc1.get_import_dict()
    fc2.load_import(data1)

    assert DeepDiff(data1, fc2.get_import_dict(), ignore_order=True) == {}


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
