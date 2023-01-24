import asyncio
import logging

from aiomisc_log import basic_config, LogFormat, create_logging_handler
from asyncpgsa import pg

from cloud.model.query_builder import get_oldest_queue_id, delete_queue
from cloud.settings import Settings


logger = logging.getLogger(__name__)


def configure_logging(settings: Settings):
    basic_config(settings.log_level, settings.log_format)

    log_format = LogFormat[settings.log_format]
    aiomisc_handler = create_logging_handler(log_format)
    aiomisc_handler.setLevel(settings.log_level.upper())
    logging.getLogger("uvicorn").handlers = [aiomisc_handler]
    logging.getLogger("uvicorn.access").handlers = [aiomisc_handler]


tasks_set: set[asyncio.Task] = set()
events: dict[int, asyncio.Event] = {}


# todo: cancel task on shutdown
# fixme: older queue table record may broke app. clear!
async def queue_worker(conn):
    counter = 0
    request_counter = 0
    while True:
        counter += 1
        if counter % 1000 == 0:
            logger.info('Queue worker listening')
        if events:
            i = await conn.fetchval(get_oldest_queue_id())
            request_counter += 1

            event = events.pop(i, None)
            if event:
                event.set()
                await conn.execute(delete_queue(i))
                logger.info('Queue worker sent %d requests to db looking for %d', request_counter, i)
                request_counter = 0
        else:
            await asyncio.sleep(0.01)

worker = None


def queue_worker_event():
    global worker
    worker = asyncio.create_task(queue_worker(pg))
    logger.info('Queue worker started')
