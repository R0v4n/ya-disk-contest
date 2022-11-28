import logging
from http import HTTPStatus

from aiohttp.web_exceptions import HTTPBadRequest, HTTPException, HTTPInternalServerError, HTTPNotFound
from aiohttp.web_middlewares import middleware
from aiohttp.web_request import Request
from aiohttp.web_response import json_response

from .handlers.payloads import dumps
from .model import ParentIdValidationError


logger = logging.getLogger(__name__)


def error_json_response(http_error_cls, message: str | None = None):
    status = HTTPStatus(http_error_cls.status_code)

    error = {
        'code': http_error_cls.status_code,
        'message': message or status.description
    }
    return json_response(error, status=http_error_cls.status_code, dumps=dumps)


@middleware
async def error_middleware(request: Request, handler):
    try:
        return await handler(request)
    except HTTPNotFound as err:
        return error_json_response(err.__class__, 'Item not found')

    except (HTTPBadRequest, ParentIdValidationError):
        return error_json_response(HTTPBadRequest, 'Validation failed')

    except HTTPException as err:
        return error_json_response(err.__class__, err.text)

    except Exception:
        logger.exception('Unhandled exception')
        raise HTTPInternalServerError
