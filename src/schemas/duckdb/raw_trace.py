from sqlalchemy import Column, types

from src.schemas.duckdb.base import EntityMeta


class RawTrace(EntityMeta):
    __tablename__ = "raw_trace"

    block_number = Column(types.String, primary_key=True)
    data = Column(types.JSON)