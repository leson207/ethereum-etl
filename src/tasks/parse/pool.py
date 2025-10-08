import asyncio

from neo4j import AsyncDriver

from src.abis.function import FUNCTION_HEX_SIGNATURES
from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


def parse_pool(results: dict[str, list], **kwargs):
    addresses = [event["pool_address"] for event in results[Entity.EVENT]]
    addresses = list(set(addresses))
    results[Entity.POOL] = [{"address": address} for address in addresses]


# -----------------------------------------------------------


async def enrich_pool_token(
    results: dict[str, list], rpc_client: RpcClient, batch_size: int, **kwargs
):
    def _form_param_set(pools: list[dict]):
        param_sets = []
        for pool in pools:
            for func in ["token0", "token1"]:
                param_set = [
                    {
                        "to": pool["address"].lower()[:42],
                        "data": FUNCTION_HEX_SIGNATURES["erc20"][func],
                    }
                ]
                param_sets.append(param_set)

        return param_sets

    async def _run(rpc_client: RpcClient, pools: list[dict]):
        param_sets = _form_param_set(pools)
        responses = await rpc_client.eth_call(param_sets=param_sets)

        num_enrich_field = 2
        for i in range(len(pools)):
            pool = pools[i]
            if any(
                "error" in i
                for i in responses[i * num_enrich_field : (i + 1) * num_enrich_field]
            ):
                results[Entity.POOL].remove(pool)  # TODO: fix this to hold error pool?
                continue

            token0, token1 = responses[
                i * num_enrich_field : (i + 1) * num_enrich_field
            ]
            pool["token0_address"] = "0x" + token0["result"][-40:]
            pool["token1_address"] = "0x" + token1["result"][-40:]

    tasks = []
    batch_size = 10
    for i in range(0, len(results[Entity.POOL]), batch_size):
        batch = results[Entity.POOL][i : i + batch_size]
        task = asyncio.create_task(_run(rpc_client, batch))
        tasks.append(task)

    await asyncio.gather(*tasks)


# -------------------------------------------


async def enrich_pool_balance(
    results: dict[str, list], rpc_client: RpcClient, batch_size: int, **kwargs
):
    def _form_param_set(pools: list[dict]):
        param_sets = []
        for pool in pools:
            for token in ["token0_address", "token1_address"]:
                param_set = [
                    {
                        "to": "0x" + pool[token].lower()[-40:],
                        "data": FUNCTION_HEX_SIGNATURES["erc20"]["balanceOf"][:10]
                        + pool["address"].lower().replace("0x", "").rjust(64, "0"),
                    }
                ]
                param_sets.append(param_set)

        return param_sets

    async def _run(rpc_client: RpcClient, pools: list[dict]):
        param_sets = _form_param_set(pools)
        responses = await rpc_client.eth_call(param_sets=param_sets)

        num_enrich_field = 2
        for i in range(len(pools)):
            pool = pools[i]
            if any(
                "error" in i
                for i in responses[i * num_enrich_field : (i + 1) * num_enrich_field]
            ):
                continue

            balance0, balance1 = responses[
                i * num_enrich_field : (i + 1) * num_enrich_field
            ]
            pool["token0_balance"] = (
                int(balance0["result"], 16) if balance0["result"] != "0x" else 0
            )
            pool["token1_balance"] = (
                int(balance1["result"], 16) if balance1["result"] != "0x" else 0
            )

    tasks = []
    batch_size = 10
    for i in range(0, len(results[Entity.POOL]), batch_size):
        batch = results[Entity.POOL][i : i + batch_size]
        task = asyncio.create_task(_run(rpc_client, batch))
        tasks.append(task)

    await asyncio.gather(*tasks)


# -----------------------------------------


async def enrich_pool_update_graph(
    results: dict[str, list], graph_client: AsyncDriver, **kwargs
):
    async def _run(client: AsyncDriver, pool: dict):
        query = """
            MERGE (a:Token {address: $token0_address})
            MERGE (b:Token {address: $token1_address})

            MERGE (a)-[p_ab:POOL {address: $pool_address}]->(b)
            SET p_ab.src_balance = $token0_balance,
                p_ab.tgt_balance = $token1_balance

            MERGE (b)-[p_ba:POOL {address: $pool_address}]->(a)
            SET p_ba.src_balance = $token1_balance,
                p_ba.tgt_balance = $token0_balance
        """
        await client.execute_query(
            query,
            pool_address=pool["address"],
            token0_address=pool["token0_address"],
            token1_address=pool["token1_address"],
            token0_balance=str(pool["token0_balance"]),
            token1_balance=str(pool["token1_balance"]),
        )

    tasks = []
    for pool in results[Entity.POOL]:
        await _run(graph_client, pool)
    #     task = asyncio.create_task(_run(graph_client, pool))
    #     tasks.append(task)

    # await asyncio.gather(*tasks)


async def enrich_pool_price(
    results: dict[str, list], graph_client: AsyncDriver, **kwargs
):
    async def _run(client: AsyncDriver, pool: dict):
        USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower()
        query = """
            MATCH (start:Token {address: $start_address}), (end:Token {address: $end_address})
            MATCH p = (start)-[*BFS..5]-(end)
            RETURN relationships(p) AS edges
        """

        records, _, _ = await client.execute_query(
            query, start_address=pool["token0_address"], end_address=USDT_ADDRESS
        )
        # devide by 0 ?
        if records:
            price = 1.0
            for edge in records[0][0]:
                ratio = int(edge["src_balance"]) / int(edge["tgt_balance"])
                price = price / ratio

            pool["token0_usd_price"] = price
            pool["token1_usd_price"] = price * (
                pool["token0_balance"] / pool["token1_balance"]
            )
            # print(pool)
            # print()
            # print()

    tasks = []
    for pool in results[Entity.POOL]:
        await _run(graph_client, pool)
    #     task = asyncio.create_task(_run(graph_client, pool))
    #     tasks.append(task)

    # await asyncio.gather(*tasks)
