import logging

from aiomisc_log import basic_config, LogFormat, create_logging_handler

from cloud.settings import Settings


def config_logging(settings: Settings):
    basic_config(settings.log_level.name, settings.log_format.name)
    loggers = (
            logging.getLogger(name)
            for name in logging.root.manager.loggerDict
            if name.startswith("uvicorn.")
        )
    for uvicorn_logger in loggers:
        uvicorn_logger.handlers = []
    log_format = LogFormat[settings.log_format]
    logging.getLogger("uvicorn").handlers = [create_logging_handler(log_format)]
