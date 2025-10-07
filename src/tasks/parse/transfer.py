from eth_utils import keccak, to_hex

from src.utils.common import hex_to_dec
from src.utils.enumeration import Entity

TRANSFER_EVENT_TEXT_SIGNATURE = "Transfer(address,address,uint256)"  # from, to, value
TRANSFER_EVENT_HEX_SIGNATURE = to_hex(keccak(text=TRANSFER_EVENT_TEXT_SIGNATURE))


def parse_transfer(results: dict[str, list], **kwargs):
    for log in results[Entity.LOG]:
        if len(log["topics"]) == 0 or log["topics"][0] != TRANSFER_EVENT_HEX_SIGNATURE:
            continue

        transfer = parse(log)
        if transfer:
            results[Entity.TRANSFER].append(transfer)


def parse(item: dict):
    params = item["topics"][1:]
    for i in range((len(item["data"]) - 2) // 64):
        params.append(item["data"][2 + i : 2 + i + 64])

    if len(params) != 3:
        # {'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'topics': ['0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c', '0x0000000000000000000000003a10dc1a145da500d5fba38b9ec49c8ff11a981f'], 'data': '0x00000000000000000000000000000000000000000000000001ec67f735de8000', 'block_hash': '0x875f4b4a5f5da1012e932037b9b90caad54c322fed5b7284ba80a70414a3eada', 'block_number': 23170030, 'block_timestamp': 1755545267, 'transaction_hash': '0xc4903aa1b2dc23dbed708a4dc0b48e1b643989a4a20d1d20667306ef43f93ada', 'transaction_index': 0, 'log_index': 0, 'removed': False}
        raise
        # return None

    from_address = "0x" + params[0][-40:]
    to_address = "0x" + params[1][-40:]
    value = params[2]
    transfer = {
        "contract_address": item["address"],
        "from_address": from_address,
        "to_address": to_address,
        "value": hex_to_dec(value),
        "transaction_hash": item["transaction_hash"],
        "log_index": item["log_index"],
        "block_number": item["block_number"],
    }

    return transfer
