from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


async def fetch_raw_block(
    results: dict[str, list],
    rpc_client: RpcClient,
    block_numbers: list[int],
    include_transaction: bool,
    **kwargs
):
    responses = await rpc_client.get_block_by_number(
        block_numbers=block_numbers, include_transaction=include_transaction
    )

    raw_blocks = [
        {
            "block_number": block_number,
            "included_transaction": True,
            "data": data["result"],
        }
        for block_number, data in zip(block_numbers, responses)
    ]
    results[Entity.RAW_BLOCK] = raw_blocks
