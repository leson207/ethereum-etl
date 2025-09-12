import os
from functools import lru_cache
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


@lru_cache
def get_env_filename():
    runtime_env = os.getenv("ENV")
    return f".env.{runtime_env}" if runtime_env else ".env"


class EnvironmentSettings(BaseSettings):
    CLICKHOUSE_SERVER: str
    CLICKHOUSE_USERNAME: str
    CLICKHOUSE_PASSWORD:str
    DATABASE_NAME: str = Field(default_factory=str)

    KAFKA_SERVER: str

    PROVIER_URIS: list
    WEBSOCKET_URL: str

    ETHERSCAN_API_KEY: str

    ENVIRONMENT_NAME: str
    DEBUG_MODE: bool

    def __init__(self, **data):
        super().__init__(**data)
        self._project_root = Path(__file__).resolve().parent.parent.parent
        self._local_log_folder = self._project_root / "artifacts" / "logs"
        self._local_database_folder = self._project_root / "artifacts" / "dbs"
        self._local_data_folder = self._project_root / "artifacts" / "data"

    @model_validator(mode="after")
    def compute(self):
        self.DATABASE_NAME = self.DATABASE_NAME + self.ENVIRONMENT_NAME
        return self

    model_config = SettingsConfigDict(
        env_file=get_env_filename(), env_file_encoding="utf-8"
    )


env = EnvironmentSettings()
