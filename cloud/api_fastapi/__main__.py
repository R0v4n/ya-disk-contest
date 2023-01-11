import typer
import uvicorn

from cloud.settings import default_settings, Settings
from cloud.utils.typer_meets_pydantic import TyperEntryPoint


@TyperEntryPoint(default_settings)
def _main(settings: Settings):

    # app = create_app(settings)
    uvicorn.run(
        'cloud.api_fastapi.app:app',
        host=str(settings.api_address),
        port=settings.api_port,
        log_level=settings.log_level,
        workers=settings.api_workers
    )


def main():
    typer.run(_main)


if __name__ == "__main__":
    main()

