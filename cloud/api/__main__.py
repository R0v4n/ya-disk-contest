from aiohttp import web

from cloud.api.app import create_app
from cloud.utils.arguments_parse import clear_environ
from cloud.utils.typer_meets_pydantic import typer_entry_point
from .settings import default_settings, Settings


@typer_entry_point(default_settings)
def main(args: Settings):
    """It's alive!"""
    clear_environ(lambda name: name.startswith(args.Config.env_prefix))
    # todo: add logging and socket. how to change user on Windows?
    app = create_app(args)
    web.run_app(app, host=str(args.api_address), port=args.api_port)


if __name__ == '__main__':
    main()
