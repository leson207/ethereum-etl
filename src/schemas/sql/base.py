from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base

metadata = MetaData()
EntityMeta = declarative_base(metadata=metadata)


def init(engine):
    EntityMeta.metadata.create_all(bind=engine)
