from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


async def fetch_raw_trace(
    client: RpcClient, results: dict[str, list], block_numbers: list[int], **kwargs
):
    responses = await client.get_trace_by_block_number(block_numbers=block_numbers)

    for block_number, response in zip(block_numbers, responses):
        for data in response["result"]:
            transaction_hash = data["transactionHash"]
            raw_trace = {
                "block_number": block_number,
                "transaction_hash": transaction_hash,
                "data": data,
            }
            results[Entity.RAW_TRACE].append(raw_trace)
    
    print(len(results[Entity.RAW_TRACE]))
