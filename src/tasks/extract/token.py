import asyncio

from neo4j import AsyncDriver

from src.abis.function import FUNCTION_HEX_SIGNATURES
from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


def token_init_address(results: dict[str, list], **kwargs):
    addresses = [
        addr
        for pool in results[Entity.POOL]
        for addr in (pool["token0_address"], pool["token1_address"])
    ]
    addresses = list(set(addresses))
    results[Entity.TOKEN] = [{"address": address} for address in addresses]


# ------------------------------------------


async def token_enrich_info(
    results: dict[str, list], rpc_client: RpcClient, batch_size: int, **kwargs
):
    def decode(hex_string):
        data = bytes.fromhex(hex_string[2:])

        # string length is at byte 32..64
        length = int.from_bytes(data[32:64], "big")

        # string content is at byte 64..64+length
        string_bytes = data[64 : 64 + length]
        decoded = string_bytes.decode()
        return decoded

    def _form_param_set(tokens):
        param_sets = []
        for token in tokens:
            for func in ["name", "symbol", "decimals", "totalSupply"]:
                param_set = [
                    {
                        "to": "0x" + token["address"].lower()[-40:],
                        "data": FUNCTION_HEX_SIGNATURES["erc20"][func],
                    }
                ]
                param_sets.append(param_set)

        return param_sets

    async def _run(rpc_client: RpcClient, tokens: list[dict]):
        param_sets = _form_param_set(tokens)
        responses = await rpc_client.eth_call(param_sets=param_sets)

        for i in range(0, len(tokens)):
            token = tokens[i]
            if any("error" in i for i in responses[i * 4 : (i + 1) * 4]):
                continue

            name, symbol, decimals, total_supply = responses[i * 4 : (i + 1) * 4]
            token["name"] = decode(name["result"])
            token["symbol"] = decode(symbol["result"])
            token["decimals"] = (
                int(decimals["result"], 16) if decimals["result"] != "0x" else 0
            )
            token["total_supply"] = (
                int(total_supply["result"], 16) if total_supply["result"] != "0x" else 0
            )

    tasks = []
    batch_size = 5
    for i in range(0, len(results[Entity.TOKEN]), batch_size):
        batch = results[Entity.TOKEN][i : i + batch_size]
        task = asyncio.create_task(_run(rpc_client, batch))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def token_update_graph(
    results: dict[str, list], graph_client: AsyncDriver, **kwargs
):
    async def _run(tokens: list[dict]):
        params = {
            "tokens": [
                {
                    "address": token["address"].lower(),
                    "name": token["name"],
                    "symbol": token["symbol"],
                    "decimals": token["decimals"],
                }
                for token in tokens
            ]
        }

        query = """
            UNWIND $tokens AS token
            MERGE (t:TOKEN {address: token.address})
            SET t.name = token.name,
                t.symbol = token.symbol,
                t.decimals = token.decimals
        """

        await graph_client.execute_query(query, **params)

    tasks = []
    BATCH_SIZE = 500
    for i in range(0, len(results[Entity.TOKEN]), BATCH_SIZE):
        batch = results[Entity.TOKEN][i : i + BATCH_SIZE]
        task = asyncio.create_task(_run(batch))
        tasks.append(task)

    await asyncio.gather(*tasks)
