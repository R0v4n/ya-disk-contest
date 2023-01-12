import logging
from http import HTTPStatus

from aiohttp.web_exceptions import HTTPBadRequest, HTTPException, HTTPInternalServerError, HTTPNotFound
from aiohttp.web_middlewares import middleware
from aiohttp.web_request import Request

from cloud.model import ParentIdValidationError

logger = logging.getLogger(__name__)


def format_http_error(http_error_cls, message: str | None = None):
    status = HTTPStatus(http_error_cls.status_code)

    error = {
        'code': http_error_cls.status_code,
        'message': message or status.description
    }
    return http_error_cls(body=error)


@middleware
async def error_middleware(request: Request, handler):
    try:
        return await handler(request)
    except HTTPNotFound as err:
        raise format_http_error(err.__class__, 'Item not found')

    except (HTTPBadRequest, ParentIdValidationError):
        raise format_http_error(HTTPBadRequest, 'Validation failed')

    except HTTPException as err:
        raise format_http_error(err.__class__, err.text)

    except Exception:
        logger.exception('Unhandled exception')
        raise format_http_error(HTTPInternalServerError)
