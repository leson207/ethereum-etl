from src.fetchers.rpc_client import RPCClient
from src.abis.function import FUNCTION_HEX_SIGNATURES, decode_function_output


class TokenService:
    def __init__(self, client: RPCClient):
        self.client = client

    async def get_token_name(self, token_address, erc="erc20"):
        data = (
            "eth_call",
            [{"to": token_address, "data": FUNCTION_HEX_SIGNATURES[erc]["name"]}],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "name", res["result"])
        return res["name"]

    async def get_token_symbol(self, token_address, erc="erc20"):
        data = (
            "eth_call",
            [{"to": token_address, "data": FUNCTION_HEX_SIGNATURES[erc]["symbol"]}],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "symbol", res["result"])
        return res["symbol"]

    async def get_token_decimals(self, token_address, erc="erc20"):
        data = (
            "eth_call",
            [{"to": token_address, "data": FUNCTION_HEX_SIGNATURES[erc]["decimals"]}],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "decimals", res["result"])
        return res["decimals"]

    async def get_token_total_supply(self, token_address, erc="erc20"):
        data = (
            "eth_call",
            [
                {
                    "to": token_address,
                    "data": FUNCTION_HEX_SIGNATURES[erc]["totalSupply"],
                }
            ],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "totalSupply", res["result"])
        return res["totalSupply"]
