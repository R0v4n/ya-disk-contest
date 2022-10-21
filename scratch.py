import asyncio
from dataclasses import dataclass
from functools import reduce
from typing import Iterable

from cloud.utils.pg import DEFAULT_PG_URL


async def f():
    await asyncio.sleep(2)
    print('222')
    return 1


async def f2():
    await asyncio.sleep(5)
    print('555')
    return 1


# loop = asyncio.get_event_loop()
# f = asyncio.gather(f2(), f())
# loop.run_until_complete(f)

if __name__ == '__main__':
    #
    # import argparse
    #
    # parser = argparse.ArgumentParser()
    # parser.add_argument('echo')
    # args = parser.parse_args()
    # print(args.echo)
    # a, b = {1, 2}
    # print(a, b)
    from yarl import URL
    url = URL(DEFAULT_PG_URL)
    print(url, type(url))