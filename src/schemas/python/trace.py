from typing import Optional

from src.schemas.python.base import BaseSchema

# https://docs.metamask.io/services/reference/ethereum/json-rpc-methods/trace-methods/trace_block/
class Trace(BaseSchema):
    action: dict
    block_hash: str
    block_number: int
    error: Optional[str]
    result: Optional[dict]
    subtraces: int
    trace_address: Optional[list[int]]
    transaction_hash: str
    transaction_position: int
    type: str

    _num_fields = ("block_number", "subtraces", "transaction_position")

    _address_fields = ()
