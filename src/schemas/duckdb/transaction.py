from sqlalchemy import JSON, Column, DateTime, Integer, String

from src.schemas.duckdb.base import EntityMeta


class Transaction(EntityMeta):
    __tablename__ = "transaction"

    # Core transaction fields
    transaction_type = Column(Integer)
    nonce = Column(Integer)
    gas = Column(Integer)

    max_fee_per_gas = Column(Integer)
    max_priority_fee_per_gas = Column(Integer)
    max_fee_per_blob_gas = Column(Integer)

    to_address = Column(String)
    from_address = Column(String)

    value = Column(Integer)
    input = Column(String)

    transaction_hash = Column(String, primary_key=True)
    block_hash = Column(String)
    block_number = Column(Integer)
    transaction_index = Column(Integer)

    gas_price = Column(Integer)

    # Optional blob-related
    blob_versioned_hashes = Column(JSON)

    # Metadata
    updated_time = Column(DateTime)
