from sqlalchemy import Column, Integer, String, JSON
from src.schemas.duckdb.base import EntityMeta


class Trace(EntityMeta):
    __tablename__ = "trace"

    action = Column(JSON)

    block_hash = Column(String)
    block_number = Column(Integer)

    error = Column(String)
    result = Column(JSON)

    subtraces = Column(Integer, primary_key=True)
    trace_address = Column(JSON)

    transaction_hash = Column(String, primary_key=True)
    transaction_position = Column(Integer, primary_key=True)

    type = Column(String)