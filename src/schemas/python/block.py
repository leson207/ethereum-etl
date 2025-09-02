from typing import Optional

from src.schemas.python.base import BaseSchema


# https://docs.metamask.io/services/reference/ethereum/json-rpc-methods/eth_getblockbynumber/
class Block(BaseSchema):
    number: int
    hash: str
    mix_hash: Optional[str]
    parent_hash: str
    nonce: str
    sha3_uncles: str
    logs_bloom: str
    transactions_root: str
    state_root: str
    receipts_root: str
    miner: str
    difficulty: int
    total_difficulty: Optional[int]
    extra_data: str
    size: int
    gas_limit: int
    gas_used: int
    timestamp: int
    uncles: Optional[list]

    transaction_count: int

    base_fee_per_gas: Optional[int]
    withdrawals_root: Optional[str]
    withdrawal_count: int

    blob_gas_used: Optional[int]
    excess_blob_gas: Optional[int]
    parent_beacon_block_root: Optional[str]
    requests_hash: Optional[str]

    transaction_count: int
    withdrawal_count: int

    _num_fields = (
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
    )

    _address_fields = ()
