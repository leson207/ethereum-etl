from src.utils.common import hex_to_dec
from src.utils.enumeration import Entity


def block_init(results: dict[str, list], **kwargs):
    for raw_block in results[Entity.RAW_BLOCK]:
        block = _extract(raw_block["data"])
        results[Entity.BLOCK].append(block)


def _extract(item: dict):
    block = {
        "hash": item["hash"],
        "parent_hash": item["parentHash"],
        "sha3_uncles": item["sha3Uncles"],
        "miner": item["miner"],
        "state_root": item["stateRoot"],
        "transactions_root": item["transactionsRoot"],
        "receipts_root": item["receiptsRoot"],
        "logs_bloom": item["logsBloom"],
        "difficulty": item["difficulty"],
        "number": hex_to_dec(item["number"]),
        "gas_limit": item["gasLimit"],
        "gas_used": item["gasUsed"],
        "timestamp": hex_to_dec(item["timestamp"]),
        "extra_data": item["extraData"],
        "mix_hash": item["mixHash"],
        "nonce": item["nonce"],
        "base_fee_per_gas": item["baseFeePerGas"],
        "withdrawals_root": item["withdrawalsRoot"],
        "blob_gas_used": item["blobGasUsed"],
        "excess_blob_gas": item["excessBlobGas"],
        "parent_beacon_block_root": item["parentBeaconBlockRoot"],
        "requests_hash": item["requestsHash"],
        "size": item["size"],
        "uncles": item["uncles"],
        "total_difficulty": item.get("totalDifficulty"),
        "transaction_count": len(item["transactions"]),
        "withdrawal_count": len(item.get("withdrawals", [])),
    }
    return block
