import os
from sys import argv

import forklib
from setproctitle import setproctitle
from aiohttp import web
from aiomisc import bind_socket
from aiomisc_log import basic_config
from rich import print

from cloud.api.app import create_app
from cloud.utils.arguments_parse import clear_environ
from cloud.utils.typer_meets_pydantic import typer_entry_point
from cloud.api.settings import default_settings, Settings


# todo:
#  -think about to change ids from string to int or uuid.
#  -need to handle concurrent imports order somehow. queue is doing the job, but it is bad solution.
#  -probably refactor to v 0.1.1 with more realistic imports and blocking only one folder branch in db.
#  -Also in this case refactor history for folder
#  - bind socket? forklib?
#  - how to wait until db is ready?
#  -run migrations from container


@typer_entry_point(default_settings)
def main(args: Settings):
    """It's alive!"""
    print(args)
    clear_environ(lambda name: name.startswith(args.Config.env_prefix))

    basic_config(args.log_level.name, args.log_format.name)

    sock = bind_socket(address=str(args.api_address), port=args.api_port,
                       proto_name='http')

    setproctitle(f'[Master] {os.path.basename(argv[0])}')

    def worker():
        setproctitle(f'[Worker] {os.path.basename(argv[0])}')
        app = create_app(args)
        web.run_app(app, sock=sock)

    forklib.fork(os.cpu_count(), worker, auto_restart=True)

    # app = create_app(args)
    # web.run_app(app, host=str(args.api_address), port=args.api_port)


if __name__ == '__main__':
    main()
