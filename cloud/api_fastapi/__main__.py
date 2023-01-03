import os
from functools import partial

import uvicorn
from rich import print
from fastapi import FastAPI

from cloud.api_fastapi.app import startup_pg, shutdown_pg
from cloud.api_fastapi.routers import router
from cloud.settings import default_settings, Settings
from cloud.utils.typer_meets_pydantic import TyperEntryPoint

args = default_settings
dsn = os.getenv('CLOUD_PG_DSN')
args.pg_dsn = dsn
print(args)
app = FastAPI(docs_url='/')
app.include_router(router)
app.on_event('startup')(partial(startup_pg, args))
app.on_event('shutdown')(shutdown_pg)


# @TyperEntryPoint(default_settings)
# def main(args: Settings):
#
#     app.include_router(router)
#     app.on_event('startup')(partial(startup_pg, args))
#     app.on_event('shutdown')(shutdown_pg)
#
#     args.api_workers_count = 16
#     uvicorn.run(
#         'cloud.api_fastapi.__main__:app',
#         host=str(args.api_address),
#         port=args.api_port,
#         log_level=args.log_level,
#         workers=args.api_workers_count
#     )


if __name__ == "__main__":
    uvicorn.run(
        'cloud.api_fastapi.__main__:app',
        host=str(args.api_address),
        port=args.api_port,
        log_level=args.log_level,
        workers=args.api_workers_count
    )
