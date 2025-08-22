from sqlalchemy import Column, DateTime, Integer, String

from src.schemas.duckdb.base import EntityMeta


class Withdrawl(EntityMeta):
    __tablename__ = "withdrawal"

    # Foreign keys
    block_hash = Column(String)
    block_number = Column(Integer)

    # Core fields
    index = Column(Integer, primary_key=True)
    validator_index = Column(Integer)
    address = Column(String)
    amount = Column(Integer)

    # Metadata
    updated_time = Column(DateTime)
