import os
import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy_utils import create_database, drop_database
from yarl import URL

from cloud.settings import Settings
from cloud.utils.pg import make_alembic_config
from cloud.utils.testing import FakeCloud

PG_DSN = os.getenv('CI_CLOUD_PG_DSN', Settings().pg_dsn)


def pytest_addoption(parser):
    parser.addoption(
        "--aiohttp", action="store_true",
        help="test aiohttp api client (without flag testing fastapi client)"
    )
    parser.addoption(
        "--slow", action="store_true",
        help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--slow"):
        return
    skip_slow = pytest.mark.skip(reason="need --slow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture
def is_aiohttp(request):
    return request.config.getoption("--aiohttp")


@pytest.fixture
def postgres():
    tmp_name = f'{uuid.uuid4().hex}.pytest'
    tmp_url = str(URL(PG_DSN).with_path(tmp_name))
    create_database(tmp_url)

    try:
        yield tmp_url
    finally:
        drop_database(tmp_url)


@pytest.fixture
def alembic_config(postgres):
    cmd_options = SimpleNamespace(
        config='alembic.ini',
        name='alembic',
        pg_dsn=postgres,
        raiseerr=False,
        x=None
    )
    config = make_alembic_config(cmd_options)

    return config


@pytest.fixture
def fake_cloud():
    return FakeCloud()

