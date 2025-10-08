from src.utils.common import hex_to_dec
from src.utils.enumeration import Entity


def withdrawal_init(results: dict[str, list], **kwargs):
    for raw_block in results[Entity.RAW_BLOCK]:
        block_number = hex_to_dec(raw_block["data"]["number"])
        for raw_withdrawal in raw_block["data"].get("withdrawals", []):
            withdrawal = _extract(raw_withdrawal, block_number=block_number)
            results[Entity.WITHDRAWAL].append(withdrawal)


def _extract(item: dict, block_number: int):
    withdrawal = {
        "block_number": block_number,
        "index": hex_to_dec(item["index"]),
        "validator_index": item["validatorIndex"],
        "address": item["address"],
        "amount": item["amount"],
    }
    return withdrawal
