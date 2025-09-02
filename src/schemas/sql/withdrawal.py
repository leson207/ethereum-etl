from sqlalchemy import Column, Integer, String

from src.schemas.sql.base import EntityMeta


class Withdrawal(EntityMeta):
    __tablename__ = "withdrawal"

    block_hash = Column(String, primary_key=True)
    block_number = Column(Integer, primary_key=True)

    # Core fields
    index = Column(Integer, primary_key=True)
    validator_index = Column(Integer)
    address = Column(String)
    amount = Column(Integer)
