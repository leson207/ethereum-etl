from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


async def raw_block_init(
    results: dict[str, list],
    rpc_client: RpcClient,
    block_numbers: list[int],
    include_transaction: bool,
    **kwargs
):
    responses = await rpc_client.get_block_by_number(
        block_numbers=block_numbers, include_transaction=include_transaction
    )
    while "error" in responses[0] or responses[0]["result"] is None:
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
