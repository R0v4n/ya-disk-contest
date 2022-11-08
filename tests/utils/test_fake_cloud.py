import pytest
from deepdiff import DeepDiff


from tests.api.datasets import dataset_for_post_import


def test_with_static_data(fake_cloud):
    dataset = dataset_for_post_import()
    for data in dataset.import_dicts:
        fake_cloud.load_import(data)
        assert DeepDiff(data, fake_cloud.get_import_dict(), ignore_order=True) == {}

    assert DeepDiff(dataset.expected_tree, fake_cloud.get_tree(dataset.node_id), ignore_order=True) == {}
    assert DeepDiff(fake_cloud.get_all_history(), dataset.expected_history, ignore_order=True) == {}


datasets = [
    # empty
    (tuple(), ),
]


def test_generate_import(fake_cloud):
    # todo. maybe. someday.
    pass


@pytest.fixture
def cloud(fake_cloud):
    fake_cloud.generate_import()
    fake_cloud.generate_import([[1]])
    d1 = fake_cloud.get_node_copy('d1')

    fake_cloud.generate_import([1], parent_id=d1.id)

    return fake_cloud


def test_delete(cloud):
    pass