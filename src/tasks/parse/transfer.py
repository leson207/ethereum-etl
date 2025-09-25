from eth_utils import keccak, to_hex

from src.schemas.python.transfer import Transfer
from src.utils.enumeration import EntityType

TRANSFER_EVENT_TEXT_SIGNATURE = "Transfer(address,address,uint256)"  # from, to, value
TRANSFER_EVENT_HEX_SIGNATURE = to_hex(keccak(text=TRANSFER_EVENT_TEXT_SIGNATURE))
TRANSFER_EVENT_HEX_SIGNATURE = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


def parse_transfer(results: dict):
    for log in results[EntityType.LOG]:
        transfer = parse(log)
        if len(log["topics"]) == 0 or log["topics"][0] != TRANSFER_EVENT_HEX_SIGNATURE:
            continue
        results[EntityType.TRANSFER].append(transfer)


def parse(item: dict):
    params = item["topics"][1:]
    for i in range((len(item["data"]) - 2) // 64):
        params.append(item["data"][2 + i : 2 + i + 64])

    if len(params) != 3:
        raise

    from_address = "0x" + params[0][-40:]
    to_address = "0x" + params[1][-40:]
    value = params[2]

    transfer = Transfer(
        contract_address=item["address"],
        from_address=from_address,
        to_address=to_address,
        value=value,
        transaction_hash=item["transactionHash"],
        log_index=item["logIndex"],
        block_number=item["blockNumber"],
    )

    return transfer.model_dump()
