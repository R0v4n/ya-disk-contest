import logging

from aiomisc_log import basic_config, LogFormat, create_logging_handler

from disk.settings import Settings

logger = logging.getLogger(__name__)


def configure_logging(settings: Settings):
    basic_config(settings.log_level, settings.log_format)

    log_format = LogFormat[settings.log_format]
    aiomisc_handler = create_logging_handler(log_format)
    aiomisc_handler.setLevel(settings.log_level.upper())
    logging.getLogger("uvicorn").handlers = [aiomisc_handler]
    logging.getLogger("uvicorn.access").handlers = [aiomisc_handler]

