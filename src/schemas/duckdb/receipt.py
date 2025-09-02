from sqlalchemy import Column, Integer, String, Boolean
from src.schemas.duckdb.base import EntityMeta


class Receipt(EntityMeta):
    __tablename__ = "receipt"

    block_hash = Column(String)
    block_number = Column(Integer)

    contract_address = Column(String)
    cumulative_gas_used = Column(Integer)
    from_address = Column(String)
    gas_used = Column(Integer)
    effective_gas_price = Column(Integer)
    log_count = Column(Integer)
    logs_bloom = Column(String)
    status = Column(Boolean)
    to_address = Column(String)
    transaction_hash = Column(String, primary_key=True)
    transaction_index = Column(Integer)
    type = Column(Integer)
