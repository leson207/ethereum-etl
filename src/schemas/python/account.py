from src.schemas.python.base import BaseSchema


class Account(BaseSchema):
    address: str

    _num_fields = ()

    _address_fields = ("address",)
