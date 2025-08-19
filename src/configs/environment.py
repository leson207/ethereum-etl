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
    DATABASE_DIALECT: str
    DATABASE_PREFIX: str
    DATABASE_SUFFIX: str
    DATABASE_NAME: str = Field(default_factory=str)

    CLICKHOUSE_SERVER: str
    CLICKHOUSE_USERNAME: str
    CLICKHOUSE_PASSWORD:str
    KAFKA_SERVER: str

    NETWORKS: list[str]
    DEBUG_MODE: bool

    def __init__(self, **data):
        super().__init__(**data)
        self._project_root = Path(__file__).resolve().parent.parent.parent
        self._local_log_folder = self._project_root / "artifacts" / "logs"
        self._local_database_folder = self._project_root / "artifacts" / "dbs"
        self._local_data_folder = self._project_root / "artifacts" / "data"

    @model_validator(mode="after")
    def compute(self):
        self.DATABASE_NAME = self.DATABASE_PREFIX + self.DATABASE_SUFFIX
        return self

    model_config = SettingsConfigDict(
        env_file=get_env_filename(), env_file_encoding="utf-8"
    )


env = EnvironmentSettings()
