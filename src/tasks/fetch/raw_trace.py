from src.clients.rpc_client import RpcClient
from src.utils.enumeration import EntityType


async def fetch_raw_trace(client: RpcClient, results: dict, block_numbers: list[int]):
    responses = await client.get_trace_by_block_number(block_numbers=block_numbers)

    raw_traces = [
        {"block_number": block_number, "data": data}
        for block_number, data in zip(block_numbers, responses)
    ]
    results[EntityType.RAW_TRACE] = raw_traces
