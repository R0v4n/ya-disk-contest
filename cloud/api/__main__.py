import argparse

from aiohttp import web
from configargparse import ArgumentParser
# noinspection PyPackageRequirements
from yarl import URL

from cloud.api.app import create_app
from cloud.utils.arguments_parse import positive_int, clear_environ
from cloud.utils.pg import DEFAULT_PG_URL

# todo:
#  -write configs
#  -validation
#  -add logging
#  -drop enum type in alembic config
#  -think about to change ids from string to int or uuid. does is matter?
#  -remove sec fractions from db
#  -00:00 to Z in GET/nodes/id response json


ENV_VAR_PREFIX = 'CLOUD_'


parser = ArgumentParser(
    auto_env_var_prefix=ENV_VAR_PREFIX, allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)

# parser.add_argument('--user', required=False, type=pwd.getpwnam,
#                     help='Change process UID')

group = parser.add_argument_group('API Options')
group.add_argument('--api-address', default='127.0.0.1',
                   help='IPv4/IPv6 address API server would listen on')
group.add_argument('--api-port', type=positive_int, default=8081,
                   help='TCP port API server would listen on')

group = parser.add_argument_group('PostgreSQL options')
group.add_argument('--pg-url', type=URL, default=URL(DEFAULT_PG_URL),
                   help='URL to use to connect to the database')
group.add_argument('--pg-pool-min-size', type=int, default=10,
                   help='Minimum database connections')
group.add_argument('--pg-pool-max-size', type=int, default=10,
                   help='Maximum database connections')

# group = parser.add_argument_group('Logging options')
# group.add_argument('--log-level', default='info',
#                    choices=('debug', 'info', 'warning', 'error', 'fatal'))
# group.add_argument('--log-format', choices=LogFormat.choices(),
#                    default='color')


def main():
    args = parser.parse_args()

    clear_environ(lambda name: name.startswith(ENV_VAR_PREFIX))
    # todo: add logging and socket. how to change user on Windows?
    app = create_app(args)
    web.run_app(app, host=args.api_address, port=args.api_port)


if __name__ == '__main__':
    main()
