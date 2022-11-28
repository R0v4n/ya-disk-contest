from enum import Enum

from pydantic import PostgresDsn, IPvAnyAddress, conint

from cloud.utils.typer_meets_pydantic import SettingsBase


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


class Settings(SettingsBase):
    api_address: IPvAnyAddress = '0.0.0.0'
    api_port: conint(gt=0, lt=2 ** 16) = 8081

    pg_dsn: PostgresDsn = 'postgresql://user:psw@localhost:5432/cloud'
    pg_pool_min_size: int = 10
    pg_pool_max_size: int = 10

    log_level: LogLevel = LogLevel.info
    log_format: LogFormat = LogFormat.color

    class Config:
        env_prefix = 'CLOUD_'
        validate_all = True

        descriptions: list[str] = [
            'IPv4/IPv6 address API server would listen on',
            'TCP port API server would listen on',
            'URL to use to connect to the database',
            'Minimum database connections',
            'Maximum database connections'
        ]


default_settings = Settings()
