from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


async def raw_trace_init(
    results: dict[str, list], rpc_client: RpcClient, block_numbers: list[int], **kwargs
):
    responses = await rpc_client.get_trace_by_block_number(block_numbers=block_numbers)

    while "error" in responses[0] or responses[0]["result"] is None:
        responses = await rpc_client.get_trace_by_block_number(block_numbers=block_numbers)

    for block_number, response in zip(block_numbers, responses):
        for data in response["result"]:
            transaction_hash = data.get("transactionHash")
            if transaction_hash:
                raw_trace = {
                    "block_number": block_number,
                    "transaction_hash": transaction_hash,
                    "data": data,
                }
                results[Entity.RAW_TRACE].append(raw_trace)
