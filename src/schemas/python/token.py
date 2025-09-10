from src.schemas.python.base import BaseSchema


class Token(BaseSchema):
    address: str
    name: str
    symbol: str
    decimals: int

    _num_fields = ("decimals")

    _address_fields = ("address", )
