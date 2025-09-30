from src.utils.common import hex_to_dec
from src.utils.enumeration import Entity


def parse_receipt(results: dict[str, list], **kwargs):
    for raw_receipt in results[Entity.RAW_RECEIPT]:
        receipt = parse(raw_receipt["data"])
        results[Entity.RECEIPT].append(receipt)


def parse(item: dict):
    receipt = {
        "block_hash": item["blockHash"],
        "block_number": hex_to_dec(item["blockNumber"]),
        "contract_address": item["contractAddress"],
        "cumulative_gas_used": item["cumulativeGasUsed"],
        "from_address": item["from"],
        "gas_used": item["gasUsed"],
        "effective_gas_price": item["effectiveGasPrice"],
        "log_count": len(item.get("logs", [])),
        "logs_bloom": item["logsBloom"],
        "status": hex_to_dec(item["status"]),
        "to_address": item["to"],
        "transaction_hash": item["transactionHash"],
        "transaction_index": item["transactionIndex"],
        "type": item["type"],
    }
    return receipt
