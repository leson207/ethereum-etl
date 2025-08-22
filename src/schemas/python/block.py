from datetime import datetime

from pydantic import BaseModel, field_validator
from typing import Optional


class Block(BaseModel):
    hash: str
    parent_hash: str
    sha3_uncles: str
    miner: str
    state_root: str
    transactions_root: str
    receipts_root: str
    logs_bloom: str
    difficulty: int
    number: int
    gas_limit: int
    gas_used: int
    timestamp: int
    extra_data: str
    mix_hash: Optional[str] = None
    nonce: str
    base_fee_per_gas: Optional[int] = None
    withdrawals_root: Optional[str] = None
    blob_gas_used: Optional[int] = None
    excess_blob_gas: Optional[int] = None
    parent_beacon_block_root: Optional[str] = None
    requests_hash: Optional[str] = None
    size: int
    uncles: Optional[list] = None
    total_difficulty: Optional[int] = None

    transaction_count: int
    withdrawal_count: int

    updated_time: datetime = datetime.now()

    @field_validator(
        "number",
        "difficulty",
        "total_difficulty",
        "size",
        "gas_limit",
        "gas_used",
        "timestamp",
        "base_fee_per_gas",
        "blob_gas_used",
        "excess_blob_gas",
        mode="before"
    )
    def hex_to_dec(cls, val, info):
        if val is None or isinstance(val, int):
            return val

        try:
            return int(val, 16)
        except Exception:
            raise Exception(f"Class: {cls.__name__} - Field: {info.field_name} - Value: {val}")