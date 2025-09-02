from typing import Optional

from src.schemas.python.base import BaseSchema


class Log(BaseSchema):
    address: str
    topics: Optional[list[str]]
    data: str
    block_hash: str
    block_number: int
    block_timestamp: Optional[int]
    transaction_hash: str
    transaction_index: int
    log_index: int
    removed: bool

    _num_fields = (
        "block_number",
        "block_timestamp",
        "transaction_index",
        "log_index",
    )

    _address_fields = ()
