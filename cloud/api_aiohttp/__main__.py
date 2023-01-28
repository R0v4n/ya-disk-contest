import os
from sys import argv

import forklib
import typer
from aiohttp import web
from aiomisc import bind_socket
from aiomisc_log import basic_config
from setproctitle import setproctitle

from .app import create_app
from cloud.settings import Settings
from cloud.utils import clear_environ, typer_entry_point


@typer_entry_point
def main(settings: Settings):
    """Run API server with aiohttp web framework"""
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


if __name__ == '__main__':
    typer.run(main)
