import os
from alembic.config import CommandLine, Config
from cloud.utils.pg import DEFAULT_PG_URL


def main():
    alembic = CommandLine()
    alembic.parser.add_argument(
        '--pg-url', default=os.getenv('CLOUD_PG_URL', DEFAULT_PG_URL),
        help='Database URL [env var: CLOUD_PG_URL]'
    )
    options = alembic.parser.parse_args()

    config = Config(file_=options.config, ini_section=options.name,
                    cmd_opts=options)

    config.set_main_option('sqlalchemy.url', options.pg_url)

    exit(alembic.run_cmd(config, options))


if __name__ == '__main__':
    main()