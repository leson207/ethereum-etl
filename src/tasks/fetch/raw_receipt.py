from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


async def fetch_raw_receipt(
    results: dict[str, list], rpc_client: RpcClient, block_numbers: list[int], **kwargs
):
    responses = await rpc_client.get_receipt_by_block_number(block_numbers=block_numbers)

    for block_number, response in zip(block_numbers, responses):
        for data in response["result"]:
            transaction_hash = data["transactionHash"]
            raw_receipt = {
                "block_number": block_number,
                "transaction_hash": transaction_hash,
                "data": data,
            }
            results[Entity.RAW_RECEIPT].append(raw_receipt)
