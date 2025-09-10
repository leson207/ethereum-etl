from src.fetchers.rpc_client import RPCClient
from src.abis.function import FUNCTION_HEX_SIGNATURES, decode_function_output


class PoolService:
    def __init__(self, client: RPCClient):
        self.client = client

    async def get_token0_address(self, pool_address, erc="erc20"):
        method="eth_call"
        params=[{"to": pool_address[:42], "data": FUNCTION_HEX_SIGNATURES[erc]["token0"][:10]}, "latest"]
        res = await self.client.send_single_request(method, params)
        print(res)
        print(params)
        res = decode_function_output(erc, "token0", res["result"])
        return res["token_address"]

    async def get_token1_address(self, pool_address, erc="erc20"):
        data = (
            "eth_call",
            [{"to": pool_address, "data": FUNCTION_HEX_SIGNATURES[erc]["token1"]}],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "token1", res["result"])
        return res["token_address"]

    async def get_pool_balance(self, pool_address, erc="erc20"):
        data = (
            "eth_call",
            [
                {
                    "to": pool_address,
                    "data": FUNCTION_HEX_SIGNATURES[erc]["balanceOf"],
                }
            ],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "balanceOf", res["result"])
        return res["balance"]
