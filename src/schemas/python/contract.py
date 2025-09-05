from src.schemas.python.base import BaseSchema
from typing import Optional

class Contract(BaseSchema):
    name: Optional[str]
    address: str
    abi: str

    _num_fields = ()

    _address_fields = ("address",)
