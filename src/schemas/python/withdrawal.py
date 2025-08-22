from datetime import datetime

from pydantic import BaseModel, field_validator


class Withdrawal(BaseModel):
    # foreign keys
    block_hash: str
    block_number: int

    index: int
    validator_index: int
    address: str
    amount: int

    updated_time: datetime = datetime.now()

    @field_validator("block_number", "index", "validator_index", "amount", mode="before")
    def hex_to_dec(cls, val, info):
        if val is None or isinstance(val, int):
            return val

        try:
            return int(val, 16)
        except Exception:
            raise Exception(f"Class: {cls.__name__} - Field: {info.field_name} - Value: {val}")
