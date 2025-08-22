from sqlalchemy import JSON, Column, DateTime, Integer, String

from src.schemas.duckdb.base import EntityMeta


class Block(EntityMeta):
    __tablename__ = "block"

    # Identifiers
    hash = Column(String, primary_key=True)
    parent_hash = Column(String)
    sha3_uncles = Column(String)
    miner = Column(String)

    # Roots
    state_root = Column(String)
    transactions_root = Column(String)
    receipts_root = Column(String)

    # Core block data
    logs_bloom = Column(String)
    difficulty = Column(Integer)
    number = Column(Integer)
    gas_limit = Column(Integer)
    gas_used = Column(Integer)
    timestamp = Column(Integer)
    extra_data = Column(String)

    mix_hash = Column(String)
    nonce = Column(String)

    base_fee_per_gas = Column(Integer)
    withdrawals_root = Column(String)
    blob_gas_used = Column(Integer)
    excess_blob_gas = Column(Integer)
    parent_beacon_block_root = Column(String)
    requests_hash = Column(String)

    size = Column(Integer)
    uncles = Column(JSON)
    total_difficulty = Column(Integer)

    # Counts
    transaction_count = Column(Integer)
    withdrawal_count = Column(Integer)

    # Metadata
    updated_time = Column(DateTime)
