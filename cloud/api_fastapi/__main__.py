import uvicorn

from cloud.api_fastapi.app import create_app
from cloud.settings import default_settings, Settings
from cloud.utils.typer_meets_pydantic import TyperEntryPoint


@TyperEntryPoint(default_settings)
def main(args: Settings):

    app = create_app(args)
    uvicorn.run(
        app,
        host=str(args.api_address),
        port=args.api_port,
        log_level=args.log_level,
        # workers=args.api_workers_count
    )


if __name__ == "__main__":
    main()
