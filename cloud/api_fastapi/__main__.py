import typer
import uvicorn

from cloud.settings import Settings
from cloud.utils.arguments_parse import set_environ
from cloud.utils.typer_meets_pydantic import typer_entry_point


@typer_entry_point
def main(settings: Settings):
    """Run API server with fastapi web framework"""

    set_environ(settings.envvars_dict())

    uvicorn.run(
        'cloud.api_fastapi.app:app',
        host=str(settings.api_address),
        port=settings.api_port,
        log_level=settings.log_level,
        workers=settings.api_workers
    )


if __name__ == "__main__":
    typer.run(main)

