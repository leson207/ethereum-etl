from datetime import datetime

from pydantic import BaseModel, field_validator
from typing import Optional

class Transaction(BaseModel):
    transaction_type: int
    # chain_id: str
    nonce: int
    gas: int
    max_fee_per_gas: Optional[int]=None
    max_priority_fee_per_gas: Optional[int]=None
    to_address: Optional[str]
    value: int
    # access_list: list[dict[str, list[str]]]
    # authorization_list: list[dict]
    input: str
    # r: str
    # s: str
    # y_parity: str
    # v: str
    transaction_hash: str
    block_hash: str
    block_number: int
    # block_timestamp
    transaction_index: int
    from_address: str
    gas_price: int
    max_fee_per_blob_gas: Optional[int]=None
    blob_versioned_hashes: Optional[list[str]]=None

    updated_time: datetime = datetime.now()

    @field_validator(
        "nonce",
        "block_number",
        "transaction_index",
        "value",
        "gas",
        "gas_price",
        "max_fee_per_gas",
        "max_priority_fee_per_gas",
        "transaction_type",
        "max_fee_per_blob_gas",
        mode="before",
    )
    def hex_to_dec(cls, val, info):
        if val is None or isinstance(val, int):
            return val

        try:
            return int(val, 16)
        except Exception:
            raise Exception(f"Class: {cls.__name__} - Field: {info.field_name} - Value: {val}")

    @field_validator("to_address", "from_address", mode="before")
    def normalize_address(cls, val):
        if val is None:
            return None
        else:
            return val.lower()
