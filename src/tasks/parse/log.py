from src.schemas.python.log import Log
from src.utils.enumeration import EntityType


def parse_log(results: dict):
    for raw_receipt in results[EntityType.RECEIPT]:
        for raw_log in raw_receipt["logs"]:
            log = parse(raw_log)
            results[EntityType.LOG].append(log)


def parse(item: dict):
    log = Log(
        address=item.get("address"),
        topics=item.get("topics"),
        data=item.get("data"),
        block_hash=item.get("blockHash"),
        block_number=item.get("blockNumber"),
        block_timestamp=item.get("blockTimestamp"),
        transaction_hash=item.get("transactionHash"),
        transaction_index=item.get("transactionIndex"),
        log_index=item.get("logIndex"),
        removed=item.get("removed"),
    )

    return log.model_dump()
