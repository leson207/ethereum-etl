from datetime import datetime

import orjson
from pydantic import BaseModel, field_validator


class RawBlock(BaseModel):
    block_number: int
    included_transaction: bool
    data: dict

    updated_time: datetime = datetime.now()

    @field_validator("data", mode="before")
    def parse_dict(cls, val):
        if isinstance(val, str):
            return orjson.loads(val)

        return val
