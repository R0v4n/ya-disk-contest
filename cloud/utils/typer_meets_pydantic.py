from enum import Enum
from inspect import Parameter, Signature, get_annotations
from itertools import zip_longest
from typing import Callable, Any

import typer
from makefun import wraps
from pydantic import ValidationError, BaseSettings
from rich import print


def _typer_type(value: Any):
    """Typer by default handles only int, bool, str, Enum"""
    if type(value) in (int, bool, str) or isinstance(value, Enum):
        return type(value)
    else:
        return str


def get_base_settings_defaults(settings_type: type[BaseSettings]) -> dict[str, Any]:
    return {key: val.default for key, val in settings_type.__fields__.items()}


def build_typer_parameters(settings_type: type[BaseSettings]) -> list[Parameter]:
    kwargs = get_base_settings_defaults(settings_type)

    descriptions = getattr(settings_type.Config, 'descriptions', [])
    env_prefix = getattr(settings_type.Config, 'env_prefix', '')

    if len(kwargs) < len(descriptions):
        raise ValueError

    return [
        Parameter(
            key, kind=Parameter.POSITIONAL_OR_KEYWORD,
            default=typer.Option(val, help=descr, envvar=env_prefix + key.upper()),
            annotation=_typer_type(val)
        ) for (key, val), descr in zip_longest(kwargs.items(), descriptions)
    ]


def typer_entry_point(func: Callable[[BaseSettings], Any]):
    """
    Transforms function signature for typer, validates values received from env and cli with pydantic.

    Decorated function should have exactly one argument annotated with BaseSettings subtype.
    Each BaseSettings field will be an option with default value in cli (e.g. myapp --foo-bar).
    BaseSettings.Config can optionally contain field "descriptions: list[str]",
    that will be passed to cli option descriptions (myapp --help).

    Decorated function will receive BaseSettings instance with actual field values
    from env and cli options (or defaults).
    Also validates values with pydantic model.

    I'm just imagining how pydantic and typer work together.
    For now, this decorator support only cli options.
    """

    anno_types = tuple(get_annotations(func).values())

    if len(anno_types) != 1 or not issubclass(anno_types[0], BaseSettings):
        raise ValueError('The wrapped function must have exactly '
                         'one argument of the BaseSettings subtype')

    SettingsType: type[BaseSettings] = anno_types[0]

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


class TyperEntryPoint:
    """
    Class decorator implementation.
    """

    __slots__ = 'cli_options',

    def __init__(self, cli_options: BaseSettings):
        """
        :param cli_options: pydantic model with optional "env_prefix" and "descriptions" Config attrs.
        """
        self.cli_options = cli_options

    def __call__(self, func: Callable[[BaseSettings], Any]):
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

        return wrapper
        # typer.run(wrapper)

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


__all__ = 'typer_entry_point',


if __name__ == '__main__':
    from cloud.settings import Settings
    l = Settings.__fields__['log_format'].default
    print(l, type(l))
    print(Settings.__fields__)
