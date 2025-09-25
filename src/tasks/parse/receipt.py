from src.schemas.python.receipt import Receipt
from src.utils.enumeration import EntityType


def parse_receipt(results):
    for block_receipt in results[EntityType.RAW_RECEIPT]:
        for raw_receipt in block_receipt["data"]:
            receipt = parse(raw_receipt)
            results[EntityType.RECEIPT].append(receipt)


def parse(item: dict):
    receipt = Receipt(
        block_hash=item.get("blockHash"),
        block_number=item.get("blockNumber"),
        contract_address=item.get("contractAddress"),
        cumulative_gas_used=item.get("cumulativeGasUsed"),
        from_address=item.get("from"),
        gas_used=item.get("gasUsed"),
        effective_gas_price=item.get("effectiveGasPrice"),
        log_count=len(item.get("logs", [])),
        logs_bloom=item.get("logsBloom"),
        status=item.get("status"),
        to_address=item.get("to"),
        transaction_hash=item.get("transactionHash"),
        transaction_index=item.get("transactionIndex"),
        type=item.get("type"),
    )

    return receipt.model_dump()
