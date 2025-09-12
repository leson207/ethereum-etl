from src.abis.function import FUNCTION_HEX_SIGNATURES, decode_function_output
from src.clients.rpc_client import RpcClient


class PoolService:
    def __init__(self, client: RpcClient):
        self.client = client

    async def get_token0_address(self, pool_address, erc="erc20"):
        param_set = [
            {"to": pool_address, "data": FUNCTION_HEX_SIGNATURES[erc]["token0"][:10]}
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res=res[0]
        res = decode_function_output(erc, "token0", res["result"])
        return res["token_address"]

    async def get_token1_address(self, pool_address, erc="erc20"):
        param_set = [
            {"to": pool_address, "data": FUNCTION_HEX_SIGNATURES[erc]["token1"]}
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res=res[0]
        print(res)
        res = decode_function_output(erc, "token1", res["result"])
        return res["token_address"]

    async def get_token_balance(self, pool_address, token_address, erc="erc20"):
        param_set = [
            {
                "to": token_address,
                "data": FUNCTION_HEX_SIGNATURES[erc]["balanceOf"] + pool_address,
            }
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res=res[0]
        res = decode_function_output(erc, "balanceOf", res["result"])
        return res["balance"]

    async def get_reserve(self, pool_address, erc="erc20"):
        param_set = [
            {
                "to": pool_address,
                "data": FUNCTION_HEX_SIGNATURES[erc]["getReserves"],
            }
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res=res[0]
        res = decode_function_output(erc, "getReserves", res["result"])
        return res["reserve0"], res["reserve1"]
