from src.schemas.python.base import BaseSchema


# {'index', 'validatorIndex', 'amount', 'address'}
class Withdrawal(BaseSchema):
    # foreign keys
    block_hash: str
    block_number: int

    index: int
    validator_index: int
    address: str
    amount: int

    _num_fields = ("block_number", "index", "validator_index", "amount")

    _address_fields = ("address",)
