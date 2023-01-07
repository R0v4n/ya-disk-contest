from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound, HTTPBadRequest, HTTPInternalServerError, HTTPException
from fastapi import HTTPException as FastAPIHTTPException, FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from starlette.requests import Request

from starlette.responses import JSONResponse

from cloud.model import ParentIdValidationError

messages = {
    HTTPStatus.BAD_REQUEST: 'Validation failed',
    HTTPStatus.NOT_FOUND: 'Item not found'
}


def error_response(http_error_cls, message: str | None = None):
    status = HTTPStatus(http_error_cls.status_code)

    error = {
        'code': http_error_cls.status_code,
        'message': message or status.description
    }
    return JSONResponse(content=error, status_code=http_error_cls.status_code)


async def http_error_handler(
        _: Request,
        exc: HTTPException | FastAPIHTTPException
) -> JSONResponse:
    return error_response(exc, messages.get(exc.status_code))


async def http422_error_handler(
    _: Request,
    exc: RequestValidationError | ValidationError | ParentIdValidationError,
) -> JSONResponse:
    return error_response(HTTPBadRequest, messages.get(HTTPBadRequest.status_code))


def add_error_handlers(app: FastAPI):

    for exception in (HTTPException, FastAPIHTTPException):
        app.add_exception_handler(exception, http_error_handler)

    for exception in (RequestValidationError, ValidationError, ParentIdValidationError):
        app.add_exception_handler(exception, http422_error_handler)


__all__ = 'add_error_handlers',


