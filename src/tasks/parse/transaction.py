from src.utils.common import hex_to_dec
from src.utils.enumeration import Entity


def parse_transaction(results: dict[str, list], **kwargs):
    for raw_block in results[Entity.RAW_BLOCK]:
        for raw_transaction in raw_block["data"]["transactions"]:
            transaction = parse(raw_transaction)
            results[Entity.TRANSACTION].append(transaction)


def parse(item: dict):
    transaction = {
        "type": item["type"],
        "chain_id": item.get("chainId"),
        "nonce": item["nonce"],
        "gas": item["gas"],
        "max_fee_per_gas": item.get("maxFeePerGas"),
        "max_priority_fee_per_gas": item.get("maxPriorityFeePerGas"),
        "to_address": item["to"],
        "value": hex_to_dec(item["value"]),
        "access_list": item.get("accessList"),
        "authorization_list": item.get("authorizationList"),
        "input": item["input"],
        "r": item["r"],
        "s": item["s"],
        "y_parity": item.get("yParity"),
        "v": item["v"],
        "hash": item["hash"],
        "block_hash": item["blockHash"],
        "block_number": hex_to_dec(item["blockNumber"]),
        "transaction_index": item["transactionIndex"],
        "from_address": item["from"],
        "gas_price": item["gasPrice"],
        "max_fee_per_blob_gas": item.get("maxFeePerBlobGas"),
        "blob_versioned_hashes": item.get("blobVersionedHashes"),
    }

    return transaction
