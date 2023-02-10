import logging
import asyncio
from datetime import datetime

from asyncpg import UndefinedTableError
from asyncpgsa import PG

from disk.db.queries import import_queries


logger = logging.getLogger(__name__)


class QueueWorker:
    """this class is just my adhoc experiment to handle simultaneous imports requests."""

    __slots__ = ('_date', '_queue_id')

    _wait_queue_events: dict[int, asyncio.Event] = {}
    _release_queue_events: dict[int, asyncio.Event] = {}

    _worker_task: asyncio.Task | None = None
    _pg: PG | None = None

    def __init__(self, date: datetime):
        self._date = date
        self._queue_id = None

    async def __aenter__(self):
        self._queue_id = await self.join_queue(self._date)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._queue_id in self._release_queue_events:
            self.release_queue(self._queue_id)

    @property
    def queue_id(self):
        return self._queue_id

    @classmethod
    async def startup(cls, pg: PG, sleep_time: float):
        cls._pg = pg
        try:
            await pg.execute(import_queries.clear_queue())
        except UndefinedTableError:
            pass
        cls._worker_task = asyncio.create_task(cls.run(sleep_time))
        logger.info('Queue worker started')

    @classmethod
    async def run(cls, sleep_time: float):

        while True:
            if cls._wait_queue_events:
                queue_id = await cls._get_oldest_id()
                if queue_id is not None:
                    await cls._leave_queue(queue_id)

            await asyncio.sleep(sleep_time)

    @classmethod
    async def join_queue(cls, date: datetime):
        queue_id = await cls._insert_queue(date)
        await asyncio.sleep(0.05)
        await cls._wait_event(queue_id, cls._wait_queue_events)
        return queue_id

    @classmethod
    def release_queue(cls, queue_id: int):
        cls._release_queue_events.pop(queue_id).set()

    @classmethod
    async def _insert_queue(cls, date: datetime) -> int:
        queue_id = await cls._pg.fetchval(import_queries.insert_queue(date))
        return queue_id

    @classmethod
    async def _wait_event(cls, i: int, events_dict: dict[int, asyncio.Event]):
        event = asyncio.Event()
        events_dict[i] = event
        waiter_task = asyncio.create_task(event.wait())

        await waiter_task

    @classmethod
    async def _get_oldest_id(cls) -> int | None:
        queue_id = await cls._pg.fetchval(import_queries.get_oldest_queue_id())

        # queue_id may belong to another api worker
        event = cls._wait_queue_events.pop(queue_id, None)
        if event:
            event.set()
            return queue_id

    @classmethod
    async def _leave_queue(cls, queue_id: int):
        await cls._wait_event(queue_id, cls._release_queue_events)
        await cls._pg.execute(import_queries.delete_queue(queue_id))
