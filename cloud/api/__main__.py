import argparse

from aiohttp import web
from configargparse import ArgumentParser
from yarl import URL

from .app import create_app
from cloud.utils.arguments_parse import positive_int, clear_environ
from cloud.utils.pg import DEFAULT_PG_DSN

# todo:
#  -validation
#  -add logging
#  -think about to change ids from string to int or uuid. does is matter?
#  -need to handle concurrent imports order somehow. queue is doing the job, but it is bad solution.
#   probably refactor to v 0.1.1 with more realistic imports and blocking only one folder branch in db.
#   Also in this case refactor history for folder

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
group.add_argument('--pg-url', type=URL, default=URL(DEFAULT_PG_DSN),
                   help='URL to use to connect to the database')
group.add_argument('--pg-pool-min-size', type=int, default=20,
                   help='Minimum database connections')
group.add_argument('--pg-pool-max-size', type=int, default=20,
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
