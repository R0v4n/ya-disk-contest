from functools import partial
from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound, HTTPBadRequest, HTTPInternalServerError
from asyncpgsa import pg, PG
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError, HTTPException
from pydantic import ValidationError

from .routers import router
from cloud.settings import Settings
from cloud.model import Error, ParentIdValidationError


async def startup_pg(app: FastAPI, settings: Settings):
    app.state.pg = PG()
    await app.state.pg.init(
        str(settings.pg_dsn),
        min_size=settings.pg_pool_min_size,
        max_size=settings.pg_pool_max_size
    )
    await app.state.pg.fetchval('SELECT 1')
    print('db connected')


async def shutdown_pg(app: FastAPI):
    await app.state.pg.pool.close()
    print('db disconnected')


def create_app(settings: Settings):
    app = FastAPI(
        docs_url='/',
    )

    app.add_event_handler(
        'startup',
        partial(startup_pg, app=app, settings=settings)
    )

    app.add_event_handler(
        'shutdown',
        partial(shutdown_pg, app)
    )

    app.include_router(router)

    def error_response(http_error_cls, message: str | None = None):
        status = HTTPStatus(http_error_cls.status_code)

        error = {
            'code': http_error_cls.status_code,
            'message': message or status.description
        }
        return JSONResponse(content=error, status_code=http_error_cls.status_code)

    @app.middleware('http')
    async def error_middleware(request, call_next):
        print('middleware executing!')
        try:
            return await call_next(request)
        except HTTPNotFound as err:
            print('not found!')
            return error_response(err.__class__, 'Item not found')

        except (HTTPBadRequest, ParentIdValidationError):
            print('bad request')
            return error_response(HTTPBadRequest, 'Validation failed')

        except HTTPException as err:
            print('general http exception')
            return error_response(err, err.detail)

        except (RequestValidationError, ValidationError) as err:
            print('request validation error')
            return error_response(HTTPBadRequest, 'This should not be here!!! '+err.json())

        except Exception:
            print('internal')
            return error_response(HTTPInternalServerError)

    return app
