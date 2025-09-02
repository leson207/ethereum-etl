from typing import Optional

from src.schemas.python.base import BaseSchema


# https://docs.metamask.io/services/reference/ethereum/json-rpc-methods/eth_getblockreceipts/
class Receipt(BaseSchema):
    block_hash: str
    block_number: int
    contract_address: Optional[str]
    cumulative_gas_used: int
    from_address: str
    gas_used: int
    effective_gas_price: int
    log_count: int
    logs_bloom: str
    status: bool
    to_address: Optional[str]
    transaction_hash: str
    transaction_index: int
    type: int

    _num_fields = (
        "block_number",
        "cumulative_gas_used",
        "gas_used",
        "effective_gas_price",
        "log_count",
        "transaction_index",
        "type",
        "status"
    )

    _address_fields = ("contract_address",)
