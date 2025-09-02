from sqlalchemy import JSON, Column, Integer, String

from src.schemas.sqlite.base import EntityMeta


class Transaction(EntityMeta):
    __tablename__ = "transaction"

    type = Column(Integer)
    chain_id = Column(Integer)
    nonce = Column(Integer)
    gas = Column(Integer)
    max_fee_per_gas = Column(Integer)
    max_priority_fee_per_gas = Column(Integer)
    to_address = Column(String)
    value = Column(Integer)
    access_list = Column(JSON)
    authorization_list = Column(JSON)
    input = Column(String)
    r = Column(String)
    s = Column(String)
    y_parity = Column(String)
    v = Column(String)
    hash = Column(String, primary_key=True)
    block_hash = Column(String)
    block_number = Column(Integer)
    transaction_index = Column(Integer)
    from_address = Column(String)
    gas_price = Column(Integer)
    max_fee_per_blob_gas = Column(Integer)
    blob_versioned_hashes = Column(JSON)
