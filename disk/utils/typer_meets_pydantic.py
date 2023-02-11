from enum import Enum
from inspect import Parameter, Signature, get_annotations
from itertools import zip_longest
from typing import Callable, Any

import typer
from makefun import wraps
from pydantic import ValidationError, BaseSettings
from rich import print


def _typer_type(value: Any):
    if type(value) in (int, float, bool, str) or isinstance(value, Enum):
        return type(value)
    else:
        return str


def get_base_settings_defaults(settings_type: type[BaseSettings]) -> dict[str, Any]:
    return {key: val.default for key, val in settings_type.__fields__.items()}


def build_typer_parameters(settings_type: type[BaseSettings]) -> list[Parameter]:
    kwargs = get_base_settings_defaults(settings_type)

    descriptions = getattr(settings_type.Config, 'descriptions', [])
    env_prefix = getattr(settings_type.Config, 'env_prefix', '')
    groups = getattr(settings_type.Config, 'groups', [])

    if len(kwargs) < len(descriptions) or len(kwargs) < len(groups):
        raise ValueError

    return [
        Parameter(
            key, kind=Parameter.POSITIONAL_OR_KEYWORD,
            default=typer.Option(val, help=descr, envvar=env_prefix + key.upper(),
                                 rich_help_panel=group),
            annotation=_typer_type(val)
        ) for (key, val), descr, group in zip_longest(kwargs.items(), descriptions, groups)
    ]


def get_settings_type(func: Callable[[BaseSettings], Any]) -> type[BaseSettings]:
    anno_types = tuple(get_annotations(func).values())

    if len(anno_types) != 1 or not issubclass(anno_types[0], BaseSettings):
        raise ValueError('The wrapped function must have exactly '
                         'one argument annotated with BaseSettings subtype')

    return anno_types[0]


def typer_entry_point(func: Callable[[BaseSettings], Any]):
    """
    Transforms function signature for typer, validates values received from env and cli with pydantic.

    Decorated function should have exactly one argument annotated with BaseSettings subtype.
    Each BaseSettings field will be an option with default value in cli (e.g. my-command --any-field).

    BaseSettings.Config can optionally contain fields:
     - descriptions: list[str], that will be passed to typer.Option "help" argument.
     - groups: list[str], that will be passed to typer.Option "rich_help_panel" argument.

    Decorated function will receive BaseSettings instance with actual field values
    from env and cli options (or defaults).
    Also validates values with pydantic.

    I'm just imagining how pydantic and typer work together.
    For now, this decorator support only cli options.
    """

    SettingsType = get_settings_type(func)

    @wraps(func, Signature(build_typer_parameters(SettingsType)))
    def wrapper(**kwargs):
        try:
            settings = SettingsType(**kwargs)
        except ValidationError as err:
            print(err)
            raise typer.Abort(1)
        else:
            func(settings)

    return wrapper


__all__ = 'typer_entry_point',
