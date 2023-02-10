import typer
import uvicorn

from disk.settings import Settings
from disk.utils.arguments_parse import set_environ
from disk.utils.typer_meets_pydantic import typer_entry_point


@typer_entry_point
def main(settings: Settings):
    """Run API server with fastapi web framework"""

    set_environ(settings.envvars_dict())

    uvicorn.run(
        'disk.api_fastapi.app:app',
        host=str(settings.api_address),
        port=settings.api_port,
        log_level=settings.log_level,
        workers=settings.api_workers
    )


if __name__ == "__main__":
    typer.run(main)

