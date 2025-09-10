import asyncio

from src.abis.function import FUNCTION_HEX_SIGNATURES
from src.fetchers.rpc_client import RPCClient


async def main():
    client = RPCClient("https://eth-pokt.nodies.app")
    method = method = "eth_call"
    params = [
        {
            "to": "0x000000000000000000000000a250cc729bb3323e".lower()[:42],
            "data": FUNCTION_HEX_SIGNATURES["erc20"]["token(0)"],
        }
    ]
    res = await client.send_single_request(method, params)
    print(res)


if __name__ == "__main__":
    asyncio.run(main())

# usdc pair
0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640
0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640

# usdt pair
0x6ca298d2983ab03aa1da7679389d955a4efee15c
0x6ca298d2983ab03aa1da7679389d955a4efee15c

# pool = token0+token1+dex

# from src.repositories.sqlite.cache import CacheRepository

# repo = CacheRepository()
# repo.create()