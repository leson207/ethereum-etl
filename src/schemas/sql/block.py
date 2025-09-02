from sqlalchemy import JSON, Column, Integer, String

from src.schemas.sql.base import EntityMeta


class Block(EntityMeta):
    __tablename__ = "block"

    number = Column(Integer, primary_key=True)
    hash = Column(String, primary_key=True)
    mix_hash = Column(String)
    parent_hash = Column(String)
    nonce = Column(String)
    sha3_uncles = Column(String)
    logs_bloom = Column(String)
    transactions_root = Column(String)
    state_root = Column(String)
    receipts_root = Column(String)
    miner = Column(String)

    difficulty = Column(Integer)
    total_difficulty = Column(Integer)

    extra_data = Column(String)
    size = Column(Integer)
    gas_limit = Column(Integer)
    gas_used = Column(Integer)
    timestamp = Column(Integer)

    uncles = Column(JSON)

    transaction_count = Column(Integer)

    base_fee_per_gas = Column(Integer)
    withdrawals_root = Column(String)
    withdrawal_count = Column(Integer)

    blob_gas_used = Column(Integer)
    excess_blob_gas = Column(Integer)
    parent_beacon_block_root = Column(String)
    requests_hash = Column(String)
