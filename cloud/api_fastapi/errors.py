import logging
from http import HTTPStatus

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from cloud.model.exceptions import ModelValidationError, ItemNotFoundError


logger = logging.getLogger(__name__)

messages = {
    HTTPStatus.BAD_REQUEST: 'Validation failed',
    HTTPStatus.NOT_FOUND: 'Item not found'
}


def error_response(status_code: int, message: str | None = None):
    status = HTTPStatus(status_code)

    error = {
        'code': status_code,
        'message': message or status.phrase
    }
    return JSONResponse(content=error, status_code=status_code)


async def http_error_handler(
        _: Request,
        exc: HTTPException
) -> JSONResponse:
    return error_response(exc.status_code, exc.detail)


async def http422_error_handler(
    _: Request,
    __: RequestValidationError | ValidationError,
) -> JSONResponse:
    return error_response(HTTPStatus.BAD_REQUEST, messages.get(HTTPStatus.BAD_REQUEST))


async def model_error_handler(
    _: Request,
    exc: ItemNotFoundError | ModelValidationError,
) -> JSONResponse:
    return error_response(exc.status_code, messages.get(exc.status_code))


def add_error_handlers(app: FastAPI):

    app.add_exception_handler(RequestValidationError, http422_error_handler)

    for exception in (ItemNotFoundError, ModelValidationError):
        app.add_exception_handler(exception, model_error_handler)

    app.add_exception_handler(HTTPException, http_error_handler)


__all__ = 'add_error_handlers',

