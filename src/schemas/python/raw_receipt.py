import orjson
from pydantic import BaseModel, field_validator


class RawReceipt(BaseModel):
    block_number: int
    data: list

    @field_validator("data", mode="before")
    def parse_list(cls, val):
        if isinstance(val, str):
            return orjson.loads(val)

        return val
