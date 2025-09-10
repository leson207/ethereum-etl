from src.schemas.python.base import BaseSchema


class Pool(BaseSchema):
    pool_address: str
    token0_address: str
    token1_address: str

    _num_fields = ()

    _address_fields = ("pool_address", "token0_address", "token1_address")
