from abc import ABC, abstractmethod

from asyncpgsa.connection import SAConnection


class BaseModel(ABC):
    __slots__ = '_conn',

    def __init__(self, conn: SAConnection):
        self._conn = conn

    @property
    def conn(self) -> SAConnection:
        return self._conn


class BaseInitModel(BaseModel):
    __slots__ = ()

    @abstractmethod
    async def init(self, *args, **kwargs):
        """some initialization queries"""
