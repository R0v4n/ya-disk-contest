from enum import Enum
from inspect import Parameter, Signature
from itertools import zip_longest
from typing import Callable, Any

import typer
from makefun import wraps
from pydantic import ValidationError, BaseModel
from rich import print


class TyperEntryPoint:
    """
    Decorator class to convert a function into a typer entry point.
    Decorated function should have exactly one argument,
    that will receive cli_options instance with actual field values from env and cli (or defaults).
    Each cli_options field will be an option with default value in cli.
    Also validates values with pydantic model.

    This is a fast ad hoc implementation.
    I'm just imagining how pydantic and typer work together.
    For now, it's support only cli options.
    """

    __slots__ = 'cli_options',

    def __init__(self, cli_options: BaseModel):
        """
        :param cli_options: pydantic model with optional "env_prefix" and "descriptions" Config attrs.
        """
        self.cli_options = cli_options

    def __call__(self, func: Callable[[BaseModel], Any]):
        """
        Converting function signature for typer, validating values received from env and cli, run typer.
        Each cli_options field will be a distinct kw maybe arg with default value in the wrapper signature.
        """

        @wraps(func, Signature(self.typer_parameters))
        def wrapper(**kwargs):
            try:
                settings = self.cli_options.__class__(**kwargs)
            except ValidationError as err:
                print(err)
                raise typer.Abort(1)
            else:
                func(settings)

        typer.run(wrapper)

    @staticmethod
    def _typer_type(value: Any):
        """Typer by default handles only int, bool, str, Enum"""
        if type(value) in (int, bool, str) or isinstance(value, Enum):
            return type(value)
        else:
            return str

    @property
    def typer_parameters(self):
        items = self.cli_options.dict().items()

        descriptions = getattr(self.cli_options.Config, 'descriptions', [])
        env_prefix = getattr(self.cli_options.Config, 'env_prefix', '')

        if len(items) < len(descriptions):
            raise ValueError

        return [
            Parameter(
                key, kind=Parameter.POSITIONAL_OR_KEYWORD,
                default=typer.Option(val, help=descr, envvar=env_prefix + key.upper()),
                annotation=self._typer_type(val)
            ) for (key, val), descr in zip_longest(items, descriptions)
        ]


class LogLevel(str, Enum):
    # typer can't handle IntEnum... I'm just trying to explore tools...
    critical = 'critical'
    error = 'error'
    warning = 'warning'
    info = 'info'
    debug = 'debug'


class LogFormat(str, Enum):
    stream = 'stream'
    color = 'color'
    json = 'json'
    syslog = 'syslog'
    plain = 'plain'
    journald = 'journald'
    rich = 'rich'
    rich_tb = 'rich_tb'
