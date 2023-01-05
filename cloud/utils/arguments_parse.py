import os
from typing import Callable


def clear_environ(rule: Callable[[str], bool]):
    for name in filter(rule, tuple(os.environ)):
        os.environ.pop(name)
