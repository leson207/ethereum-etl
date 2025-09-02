from typing import Optional

from src.schemas.python.base import BaseSchema


# {'gasPrice', 'hash', 'maxFeePerBlobGas', 'input', 's', 'type', 'nonce', 'to', 'blobVersionedHashes', 'blockNumber', 'gas', 'maxFeePerGas', 'chainId', 'blockHash', 'v', 'from', 'maxPriorityFeePerGas', 'authorizationList', 'r', 'value', 'yParity', 'transactionIndex', 'accessList'}
class Transaction(BaseSchema):
    type: int
    chain_id: Optional[int]
    nonce: int
    gas: int
    max_fee_per_gas: Optional[int]
    max_priority_fee_per_gas: Optional[int]
    to_address: Optional[str]
    value: int
    access_list: Optional[list]
    authorization_list: Optional[list]
    input: str
    r: str
    s: str
    y_parity: Optional[str]
    v: str
    hash: str
    block_hash: str
    block_number: int
    transaction_index: int
    from_address: str
    gas_price: int
    max_fee_per_blob_gas: Optional[int]
    blob_versioned_hashes: Optional[list]

    _num_fields = (
        "chain_id",
        "nonce",
        "block_number",
        "transaction_index",
        "value",
        "gas",
        "gas_price",
        "max_fee_per_gas",
        "max_priority_fee_per_gas",
        "type",
        "max_fee_per_blob_gas",
    )

    _address_fields = ("to_address", "from_address")
