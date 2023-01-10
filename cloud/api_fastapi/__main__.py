import uvicorn

from cloud.api_fastapi.app import create_app
from cloud.settings import default_settings, Settings
from cloud.utils.typer_meets_pydantic import TyperEntryPoint


@TyperEntryPoint(default_settings)
def main(settings: Settings):
    app = create_app(settings)
    uvicorn.run(
        # 'cloud.api_fastapi.app:app',
        app,
        host=str(settings.api_address),
        port=settings.api_port,
        log_level=settings.log_level,
        # workers=args.api_workers_count
    )


if __name__ == "__main__":
    main()
