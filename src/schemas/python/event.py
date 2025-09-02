from src.schemas.python.base import BaseSchema


class Event(BaseSchema):
    type: str
    dex: str
    pool_address: str
    amount0_in: int
    amount1_in: int
    amount0_out: int
    amount1_out: int
    transaction_hash: str
    log_index: int
    block_number: int

    _num_fields = (
        "block_number",
        "log_index",
        "amount0_in",
        "amount1_in",
        "amount0_out",
        "amount1_out",
    )

    _address_fields = ("pool_address",)
