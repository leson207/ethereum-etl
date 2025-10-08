import asyncio
from src.utils.enumeration import Entity
from src.clients.rpc_client import RpcClient
from src.abis.function import FUNCTION_HEX_SIGNATURES


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

from neo4j import AsyncGraphDatabase, AsyncDriver

sem = asyncio.BoundedSemaphore(10)  # max 10 concurrent queries


async def enrich_pool_price_later(results: dict[str, list], **kwargs):
    # async def update(client: AsyncDriver):
    #     tasks = []
    #     query = """
    #         MERGE (a:Token {address: $token0_address})
    #         MERGE (b:Token {address: $token1_address})
    #         MERGE (a)-[p:POOL {address: $pool_address}]->(b)
    #         SET p.token0_address = $token0_address,
    #             p.token1_address = $token1_address,
    #             p.token0_balance = $token0_balance,
    #             p.token1_balance = $token1_balance
    #     """
    #     for pool in results[Entity.POOL]:
    #         task = asyncio.create_task(
    #             client.execute_query(
    #                 query,
    #                 pool_address=pool["address"],
    #                 token0_address=pool["token0_address"],
    #                 token1_address=pool["token1_address"],
    #                 token0_balance=str(pool["token0_balance"]),
    #                 token1_balance=str(pool["token1_balance"]),
    #             )
    #         )
    #         tasks.append(task)

    #     await asyncio.gather(*tasks)

    async def update(client: AsyncDriver):
        tasks = []
        query = """
            MERGE (a:Token {address: $token0_address})
            MERGE (b:Token {address: $token1_address})
            MERGE (a)-[p:POOL {address: $pool_address}]->(b)
            SET p.token0_address = $token0_address,
                p.token1_address = $token1_address,
                p.token0_balance = $token0_balance,
                p.token1_balance = $token1_balance
        """
        for pool in results[Entity.POOL]:
            res, _, _ = await client.execute_query(
                query,
                pool_address=pool["address"],
                token0_address=pool["token0_address"],
                token1_address=pool["token1_address"],
                token0_balance=str(pool["token0_balance"]),
                token1_balance=str(pool["token1_balance"]),
            )

    async def _run(client: AsyncDriver, pool: dict):
        USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        query = """
            MATCH (start:Token {address: $start_address}), (end:Token {address: $end_address})
            CALL igraphalg.get_shortest_path(start, end, NULL, false)
            YIELD path
            WITH [n IN path | id(n)] AS node_ids
            UNWIND range(0, size(node_ids) - 2) AS i
            MATCH (a)-[p:POOL]-(b)
            WHERE id(a) = node_ids[i] AND id(b) = node_ids[i + 1]
            RETURN 
                a.address AS src, 
                b.address AS tgt, 
                p.address AS pool_address,
                p.token0_address AS token0_address,
                p.token1_address AS token1_address,
                p.token0_balance AS token0_balance,
                p.token1_balance AS token1_balance
        """

        records, _, _ = await client.execute_query(
            query, start_address=pool["token0_address"], end_address=USDT_ADDRESS
        )

        # check if there is no path
        # check rounding casuse all number is int?
        # devide by zero
        price = 1.0
        for record in records:
            if record["src"] == record["token0_address"]:
                ratio = record["token0_balance"] / record["token1_balance"]
            else:
                ratio = record["token1_balance"] / record["token0_balance"]

            price = price / ratio

        pool["token0_price"] = price
        pool["token1_price"] = price / (pool["token0_balance"] / pool["token1_balance"])

    async def run(client: AsyncDriver):
        tasks = []
        for pool in results[Entity.POOL]:
            task = asyncio.create_task(_run(client, pool))
            tasks.append(task)

        await asyncio.gather(*tasks)

    URI = "bolt://localhost:7687"
    AUTH = ("", "")
    async with AsyncGraphDatabase.driver(URI, auth=AUTH) as client:
        await update(client)
        # await run(client)


async def enrich_pool_price(results: dict[str, list], **kwargs):
    async def update(client: AsyncDriver):
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
        for pool in results[Entity.POOL]:
            res, _, _ = await client.execute_query(
                query,
                pool_address=pool["address"],
                token0_address=pool["token0_address"],
                token1_address=pool["token1_address"],
                token0_balance=str(pool["token0_balance"]),
                token1_balance=str(pool["token1_balance"]),
            )

    async def _run(client: AsyncDriver, pool: dict):
        USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower()
        query = """
            MATCH (start:Token {address: $start_address}), (end:Token {address: $end_address})
            MATCH p = (start)-[*BFS]-(end)
            RETURN relationships(p) AS edges
        """

        records, _, _ = await client.execute_query(
            query, start_address=pool["token0_address"], end_address=USDT_ADDRESS
        )
        # devide by zero
        if records:
            price = 1.0
            for edge in records[0][0]:
                print(edge)
                ratio = int(edge["src_balance"]) / int(edge["tgt_balance"])
                price = price / ratio

            pool["token0_price"] = price
            pool["token1_price"] = price * (
                pool["token0_balance"] / pool["token1_balance"]
            )
            print(pool)
            print()
            print()

    async def run(client: AsyncDriver):
        for pool in results[Entity.POOL]:
            await _run(client, pool)

    URI = "bolt://localhost:7687"
    AUTH = ("", "")
    async with AsyncGraphDatabase.driver(URI, auth=AUTH) as client:
        await update(client)
        await run(client)
