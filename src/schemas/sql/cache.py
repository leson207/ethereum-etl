from sqlalchemy import Column, String

from src.schemas.sql.base import EntityMeta


class Cache(EntityMeta):
    __tablename__ = "cache"

    key = Column(String, primary_key=True)
    value = Column(String)
