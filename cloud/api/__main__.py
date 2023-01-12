import os
from sys import argv

import forklib
import typer
from aiohttp import web
from aiomisc import bind_socket
from aiomisc_log import basic_config
from setproctitle import setproctitle

from cloud.api.app import create_app
from cloud.settings import Settings
from cloud.utils.arguments_parse import clear_environ
from cloud.utils.typer_meets_pydantic import typer_entry_point


@typer_entry_point
def _main(settings: Settings):
    """It's alive!"""
    clear_environ(lambda name: name.startswith(settings.Config.env_prefix))

    basic_config(settings.log_level, settings.log_format)

    sock = bind_socket(address=str(settings.api_address), port=settings.api_port,
                       proto_name='http')

    setproctitle(f'[Master] {os.path.basename(argv[0])}')

    def worker():
        setproctitle(f'[Worker] {os.path.basename(argv[0])}')
        app = create_app(settings)
        web.run_app(app, sock=sock)

    if settings.api_workers > 1:
        setproctitle(f'[Master] {os.path.basename(argv[0])}')
        forklib.fork(settings.api_workers, worker, auto_restart=True)
    else:
        app = create_app(settings)
        web.run_app(app, sock=sock)


def main():
    typer.run(_main)


if __name__ == '__main__':
    main()
