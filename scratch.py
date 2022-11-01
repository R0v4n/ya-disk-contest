import time
from datetime import datetime, timezone, timedelta

from devtools import debug
from pydantic import BaseModel, Field


# d1 = {'1': 123, '11': '123', '22': datetime.utcnow()}
# time.sleep(0.1)
# d2 = {'11': '123', '1': 123, '21': datetime.utcnow()}
# print(d1 == d2)
# print(d1['22'])
# print(d2['21'])


class M(BaseModel):
    x: int = Field(1)
    y: int = Field(2)

    class Config:
        fields = {'x': {'exclude': True}}


m = M(x=5, y=10)
c = m.copy(update={'x': 3})
debug(c)
# debug(m.dict(include={'x'}))
dt = datetime.now(timezone.utc)
print(dt, dt + timedelta(seconds=1))