import argparse
import logging

from alembic.config import CommandLine

from disk.settings import Settings
from disk.utils.pg import make_alembic_config


def main():
    settings = Settings()
    logging.basicConfig(level=logging.DEBUG)

    alembic = CommandLine()
    alembic.parser.formatter_class = argparse.ArgumentDefaultsHelpFormatter

    alembic.parser.add_argument(
        '--pg-dsn', default=settings.pg_dsn,
        help='Database URL [env var: DISK_PG_DSN]'
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
