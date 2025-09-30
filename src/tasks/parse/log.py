from src.utils.common import hex_to_dec
from src.utils.enumeration import Entity


def parse_log(results: dict[str, list], **kwargs):
    for raw_receipt in results[Entity.RAW_RECEIPT]:
        for raw_log in raw_receipt["data"].get("logs", []):
            log = parse(raw_log)
            results[Entity.LOG].append(log)


def parse(item: dict):
    log = {
        "address": item["address"],
        "topics": item["topics"],
        "data": item["data"],
        "block_hash": item["blockHash"],
        "block_number": hex_to_dec(item["blockNumber"]),
        "block_timestamp": hex_to_dec(item["blockTimestamp"]),
        "transaction_hash": item["transactionHash"],
        "transaction_index": hex_to_dec(item["transactionIndex"]),
        "log_index": hex_to_dec(item["logIndex"]),
        "removed": item["removed"],
    }

    return log
