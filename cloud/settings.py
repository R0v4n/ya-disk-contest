import os

from pydantic import BaseModel, PostgresDsn, IPvAnyAddress, conint

from cloud.utils.typer_meets_pydantic import LogLevel, LogFormat


cpu_count = os.cpu_count() if os.name != 'nt' else 1


class Settings(BaseModel):
    api_address: IPvAnyAddress = '0.0.0.0'
    api_port: conint(gt=0, lt=2 ** 16) = 8081
    api_workers_count: conint(gt=0, le=cpu_count) = cpu_count

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
            'API client process count (default is the number of CPUs in the system)',
            'URL to use to connect to the database',
            'Minimum database connections',
            'Maximum database connections'
        ]


default_settings = Settings()
