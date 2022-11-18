import argparse
import os
from alembic.config import CommandLine

from cloud.api.settings import default_settings
from cloud.utils.pg import make_alembic_config


def main():
    # todo: add logging

    alembic = CommandLine()
    alembic.parser.formatter_class = argparse.ArgumentDefaultsHelpFormatter

    alembic.parser.add_argument(
        '--pg-dsn', default=os.getenv('CLOUD_PG_DSN', default_settings.pg_dsn),
        help='Database URL [env var: CLOUD_PG_DSN]'
    )

    options = alembic.parser.parse_args()
    if 'cmd' not in options:
        alembic.parser.error('too few arguments')
        exit(128)
    else:
        config = make_alembic_config(options)
        exit(alembic.run_cmd(config, options))


if __name__ == '__main__':
    main()
