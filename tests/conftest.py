import os
import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy_utils import create_database, drop_database
from yarl import URL

from cloud.utils.pg import DEFAULT_PG_URL, make_alembic_config
from cloud.utils.testing import FakeCloud

# fixme: CI?
PG_URL = os.getenv('CLOUD_PG_URL', DEFAULT_PG_URL)


@pytest.fixture
def postgres():
    tmp_name = f'{uuid.uuid4().hex}.pytest'
    tmp_url = str(URL(PG_URL).with_path(tmp_name))
    create_database(tmp_url)

    try:
        yield tmp_url
    finally:
        drop_database(tmp_url)


@pytest.fixture
def alembic_config(postgres):
    # todo: what is x?

    cmd_options = SimpleNamespace(
        config='alembic.ini',
        name='alembic',
        pg_url=postgres,
        raiseerr=False,
        x=None
    )
    config = make_alembic_config(cmd_options)

    # fixme: this does not work
    config.set_section_option("logger_alembic", "level", "ERROR")
    return config


@pytest.fixture
def fake_cloud():
    return FakeCloud()
