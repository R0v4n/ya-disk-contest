from aiohttp import web
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
#   Also in this case refactor history for folder


@typer_entry_point(default_settings)
def main(args: Settings):
    """It's alive!"""
    print(args)
    clear_environ(lambda name: name.startswith(args.Config.env_prefix))

    basic_config(args.log_level.name, args.log_format.name)

    # todo: add socket. how to change user on Windows?
    app = create_app(args)
    web.run_app(app, host=str(args.api_address), port=args.api_port)


if __name__ == '__main__':
    main()
