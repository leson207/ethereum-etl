from src.schemas.python.base import BaseSchema


class Transfer(BaseSchema):
    contract_address: str
    from_address: str
    to_address: str
    value: int
    transaction_hash: str
    log_index: int
    block_number: int

    _num_fields = (
        "block_number",
        "log_index",
        "value",
    )

    _address_fields = ("contract_address",)
