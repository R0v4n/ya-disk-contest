from enum import Enum
from inspect import Parameter, Signature
from itertools import zip_longest
from typing import Callable, Any

import typer
from makefun import wraps
from pydantic import ValidationError, BaseModel
from rich import print


def typer_type(value: Any):
    """Typer by default know only int, bool, str, Enum"""
    if type(value) in (int, bool, str) or isinstance(value, Enum):
        return type(value)
    else:
        return str


def typer_parameters(model: BaseModel):
    items = model.dict().items()

    descriptions = getattr(model.Config, 'descriptions', [])
    env_prefix = getattr(model.Config, 'env_prefix', '')

    if len(items) < len(descriptions):
        raise ValueError

    return [
        Parameter(
            key, kind=Parameter.POSITIONAL_OR_KEYWORD,
            default=typer.Option(val, help=descr, envvar=env_prefix + key.upper()),
            annotation=typer_type(val)
        ) for (key, val), descr in zip_longest(items, descriptions)
    ]


def typer_entry_point(cli_options: BaseModel):
    """
    Convert a function in typer entry point.
    Decorated function should have exactly one argument,
    that will receive cli_options instance with actual field values from env and cli (or defaults).
    Each cli_options field will be an option with default value in cli.
    Also validates values with pydantic.

    This is a fast ad hoc implementation.
    I'm just imagining how pydantic and typer work together.
    I tried to explore a bit how to change typer classes or functions to handle this task properly.
    Someday. Maybe. I don't have time for this right now...

    :param cli_options: pydantic model with optional "env_prefix" and "descriptions" Config attrs.
    :returns: decorator that used for entry point functions.
    """

    def change_signature(func: Callable[[BaseModel], Any]):
        """
        Converting function signature for typer and validating values received from env and cli.
        Each cli_options field will be a distinct kw maybe arg with default value in the wrapper signature.
        """

        @wraps(func, Signature(typer_parameters(cli_options)))
        def wrapper(**kwargs):
            try:
                settings = cli_options.__class__(**kwargs)
            except ValidationError as err:
                print(err)
                raise typer.Abort(1)
            else:
                func(settings)

        typer.run(wrapper)

    return change_signature
