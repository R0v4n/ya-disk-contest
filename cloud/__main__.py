import typer

from .api_aiohttp.__main__ import main as aiohttp_main
from .api_fastapi.__main__ import main as fastapi_main


def main():
    app = typer.Typer(add_completion=False)
    app.command('aiohttp')(aiohttp_main)
    app.command('fastapi')(fastapi_main)
    app()


if __name__ == '__main__':
    main()
