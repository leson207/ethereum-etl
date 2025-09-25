from src.clients.rpc_client import RpcClient
from src.utils.enumeration import EntityType


async def fetch_raw_receipt(client: RpcClient, results: dict, block_numbers: list[int]):
    responses = await client.get_receipt_by_block_number(block_numbers=block_numbers)

    raw_receipts = [
        {"block_number": block_number, "data": data}
        for block_number, data in zip(block_numbers, responses)
    ]
    results[EntityType.RAW_RECEIPT] = raw_receipts
