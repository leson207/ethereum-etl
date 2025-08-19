from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base

from src.configs.duckdb import engine

metadata = MetaData()
metadata.bind = engine
EntityMeta = declarative_base(metadata=metadata)


def init():
    EntityMeta.metadata.create_all(bind=engine)
