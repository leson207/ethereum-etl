from src.abis.function import FUNCTION_HEX_SIGNATURES, decode_function_output
from src.clients.rpc_client import RpcClient


class TokenService:
    def __init__(self, client: RpcClient):
        self.client = client

    async def get_token_name(self, token_address, erc="erc20"):
        param_set = [
            {"to": token_address, "data": FUNCTION_HEX_SIGNATURES[erc]["name"]}
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res = decode_function_output(erc, "name", res["result"])
        return res["name"]

    async def get_token_symbol(self, token_address, erc="erc20"):
        param_set = [
            {"to": token_address, "data": FUNCTION_HEX_SIGNATURES[erc]["symbol"]}
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res = decode_function_output(erc, "symbol", res["result"])
        return res["symbol"]

    async def get_token_decimals(self, token_address, erc="erc20"):
        param_set = [
            {"to": token_address, "data": FUNCTION_HEX_SIGNATURES[erc]["decimals"]}
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res = decode_function_output(erc, "decimals", res["result"])
        return res["decimals"]

    async def get_token_total_supply(self, token_address, erc="erc20"):
        param_set = [
            {"to": token_address, "data": FUNCTION_HEX_SIGNATURES[erc]["totalSupply"]}
        ]
        res = await self.client.eth_call(param_sets=[param_set])
        res = decode_function_output(erc, "totalSupply", res["result"])
        return res["totalSupply"]