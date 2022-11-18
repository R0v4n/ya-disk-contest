from pydantic import PostgresDsn, IPvAnyAddress, conint

from cloud.utils.typer_meets_pydantic import SettingsBase


class Settings(SettingsBase):
    pg_dsn: PostgresDsn = 'postgresql://rovan:hackme@localhost:5432/cloud'
    api_address: IPvAnyAddress = '127.0.0.1'
    api_port: conint(gt=0, lt=2 ** 16) = 8081
    pg_pool_min_size: int = 10
    pg_pool_max_size: int = 10

    class Config:
        env_prefix = 'CLOUD_'

        descriptions: list[str] = [
            'URL to use to connect to the database',
            'IPv4/IPv6 address API server would listen on',
            'TCP port API server would listen on',
            'Minimum database connections',
            'Maximum database connections'
        ]


default_settings = Settings()


