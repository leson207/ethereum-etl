import asyncio
import json
from src.clients.rpc_client import RpcClient


async def main():
    client = RpcClient()
    res = await client.get_receipt_by_block_number(block_numbers=[23171510])
    res = res[0]["result"]
    with open("tmp.json", "w") as f:
        json.dump(res, f, indent=4)

def tmp():
    from src.abis.event import EVENT_HEX_SIGNATURES
    print(EVENT_HEX_SIGNATURES["uniswap_v2"]["pair_created"].casefold())
    print(EVENT_HEX_SIGNATURES["uniswap_v2"]["mint"].casefold())
    print(EVENT_HEX_SIGNATURES["uniswap_v2"]["burn"].casefold())
    print(EVENT_HEX_SIGNATURES["uniswap_v2"]["swap"])

if __name__ == "__main__":
    tmp()
    # asyncio.run(main())
