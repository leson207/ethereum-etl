from src.schemas.python.base import BaseSchema


class ABI(BaseSchema):
    text_signature: str
    hex_signature: str
    data: dict

    _num_fields = ()

    _address_fields = ()
