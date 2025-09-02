import asyncio

from src.fetchers.rpc_client import RPCClient
from src.utils.function_abi import FUNCTION_HEX_SIGNATURES, decode_function_output


class Service:
    def __init__(self, client: RPCClient):
        self.client = client

    async def get_token0(self, pool_address, erc="erc20"):
        data = (
            "eth_call",
            [{"to": pool_address, "data": FUNCTION_HEX_SIGNATURES[erc]["token0"]}],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "token0", res["result"])
        return res["token_address"]

    async def get_token1(self, pool_address, erc="erc20"):
        data = (
            "eth_call",
            [{"to": pool_address, "data": FUNCTION_HEX_SIGNATURES[erc]["token1"]}],
        )
        res = await self.client.send_single_request(data)
        res = decode_function_output(erc, "token1", res["result"])
        return res["token_address"]

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


async def main():
    client = RPCClient("https://eth-pokt.nodies.app")
    service = Service(client)

    token0 = await service.get_token0("0x8e98425c52931AdC80dc732d67303c33ca1451f5")
    print(token0)

    name = await service.get_token_name("0xa6d121caede39ab5fe7bdab91918cf5254e8f306")
    print(name)

    symbol = await service.get_token_symbol(
        "0xa6d121caede39ab5fe7bdab91918cf5254e8f306"
    )
    print(symbol)

    decimals = await service.get_token_decimals(
        "0xa6d121caede39ab5fe7bdab91918cf5254e8f306"
    )
    print(decimals)

    token_supply = await service.get_token_total_supply(
        "0xa6d121caede39ab5fe7bdab91918cf5254e8f306"
    )
    print(token_supply)


if __name__ == "__main__":
    asyncio.run(main())
