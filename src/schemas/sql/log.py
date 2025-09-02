from sqlalchemy import Column, Integer, String, Boolean, JSON
from src.schemas.sql.base import EntityMeta


class Log(EntityMeta):
    __tablename__ = "log"

    address = Column(String)
    topics = Column(JSON)
    data = Column(String)

    block_hash = Column(String)
    block_number = Column(Integer)
    block_timestamp = Column(Integer)

    transaction_hash = Column(String, primary_key=True)
    transaction_index = Column(Integer)

    log_index = Column(Integer, primary_key=True)
    removed = Column(Boolean)
