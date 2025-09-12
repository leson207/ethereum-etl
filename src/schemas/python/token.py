from src.schemas.python.base import BaseSchema


class Token(BaseSchema):
    address: str
    name: str
    symbol: str
    decimals: int
    total_supply: int

    _num_fields = ("decimals", "total_supply")

    _address_fields = ("address", )
