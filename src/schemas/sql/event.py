from sqlalchemy import Column, Integer, String, Numeric
from src.schemas.sql.base import EntityMeta


class Event(EntityMeta):
    __tablename__ = "event"

    type = Column(String)
    dex =Column(String)
    pool_address = Column(String)
    amount0_in = Column(Numeric) # this value is to big for this python table to hold as int, let the database convert it
    amount1_in = Column(Numeric) # this value is to big for this python table to hold as int, let the database convert it
    amount0_out = Column(Numeric) # this value is to big for this python table to hold as int, let the database convert it
    amount1_out = Column(Numeric) # this value is to big for this python table to hold as int, let the database convert it
    transaction_hash = Column(String, primary_key=True)
    log_index = Column(Integer, primary_key=True)
    block_number = Column(Integer, primary_key=True)