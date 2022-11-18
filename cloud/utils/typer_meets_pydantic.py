from types import FunctionType
from itertools import zip_longest

import typer
from pydantic import ValidationError, BaseModel
from rich import print


class SettingsBase(BaseModel):
    class Config:
        # could be set in derived class
        env_prefix: str = ''
        descriptions: list[str] = []

    @property
    def typer_options(self):
        items = self.dict().items()

        if len(items) < len(self.Config.descriptions):
            raise ValueError

        return tuple(
            typer.Option(
                item[1],
                help=descr,
                envvar=self.Config.env_prefix + item[0].upper()
            )
            for item, descr in zip_longest(items, self.Config.descriptions)
        )

    @property
    def typer_annotations(self):
        return {key: type(val) for key, val in self.dict().items()}


# todo: fix usage message
def typer_entry_point(cli_options: SettingsBase):
    """
    Convert the function in typer entry point.
    Decorated function should have exactly one argument,
    that will receive cli_options instance with actual field values from env and cli (or defaults).
    Each cli_options field will be an option with default value in cli.
    Also validates values with pydantic.

    This is a fast ad hoc implementation.
    I'm just imagining how pydantic and typer work together.
    I tried to explore a bit how to change typer classes or functions
    (look get_params_from_function) to handle this task properly.
    Someday. Maybe. I don't have time for this right now...

    :param cli_options: pydantic model.
    :returns: decorator that used for entry point functions.
    """

    def change_signature(func: FunctionType):
        """Convert function signature for typer. Each cli_options field will be a distinct kwarg."""

        def wrapper():
            try:
                settings = cli_options.__class__(**locals())
            except ValidationError as err:
                print(err)
                raise typer.Abort(1)
            else:
                func(settings)

        annotations = cli_options.typer_annotations
        defaults = cli_options.typer_options

        code = wrapper.__code__.replace(
            co_argcount=len(defaults),
            co_varnames=tuple(annotations.keys()),
            co_nlocals=len(defaults)
        )
        new = FunctionType(
            code,
            wrapper.__globals__,
            func.__name__,
            argdefs=defaults,
            closure=wrapper.__closure__
        )

        new.__annotations__ = annotations
        new.__doc__ = func.__doc__

        return typer.run(new)

    return change_signature
