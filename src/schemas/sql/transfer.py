from sqlalchemy import Column, Integer, String, Numeric
from src.schemas.sql.base import EntityMeta


class Transfer(EntityMeta):
    __tablename__ = "transfer"

    contract_address = Column(String)
    from_address = Column(String)
    to_address = Column(String)
    value = Column(Numeric) # this value is to big for this python table to hold as int, let the database convert it
    transaction_hash = Column(String, primary_key=True)
    log_index = Column(Integer, primary_key=True)
    block_number = Column(Integer, primary_key=True)
