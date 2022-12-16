import pytest
from deepdiff import DeepDiff

from cloud.api.model import ItemType
from cloud.utils.testing import FakeCloud


def test_with_static_data(fake_cloud, dataset_for_post_import):
    dataset = dataset_for_post_import
    for data in dataset.import_dicts:
        fake_cloud.load_import(data)
        assert DeepDiff(data, fake_cloud.get_import_dict(), ignore_order=True) == {}

    assert DeepDiff(dataset.expected_tree, fake_cloud.get_tree(dataset.node_id), ignore_order=True) == {}
    assert DeepDiff(fake_cloud.get_all_history(), dataset.expected_history, ignore_order=True) == {}


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
        i = cloud[item]


def test_generate_import_parent_size():
    d1 = cloud[0]
    assert d1.size == sum(cloud[0, i].size for i in range(3))
