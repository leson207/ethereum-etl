import asyncio

from neo4j import AsyncDriver, AsyncSession

from src.abis.function import FUNCTION_HEX_SIGNATURES
from src.clients.rpc_client import RpcClient
from src.utils.enumeration import Entity


def pool_init_address(results: dict[str, list], **kwargs):
    addresses = [event["pool_address"] for event in results[Entity.EVENT]]
    addresses = list(set(addresses))
    results[Entity.POOL] = [{"address": address} for address in addresses]
    # for i in results[Entity.POOL]:
    #     if i["address"].lower()=="0xc7bbec68d12a0d1830360f8ec58fa599ba1b0e9b" and i["address"]!="0xc7bBeC68d12a0d1830360F8Ec58fA599bA1b0e9b":
    #         raise


# -----------------------------------------------------------


async def pool_enrich_token_address(
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


async def pool_enrich_token_balance(
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


async def pool_update_graph(
    results: dict[str, list], graph_client: AsyncDriver, **kwargs
):
    async def _process_batch(batch: list[dict]):
        params = {
            "pools": [
                {
                    "pool_address": pool["address"],
                    "token0_address": pool["token0_address"].lower(),
                    "token1_address": pool["token1_address"].lower(),
                    "token0_balance": str(pool["token0_balance"]),
                    "token1_balance": str(pool["token1_balance"]),
                }
                for pool in batch
            ]
        }

        query = """
        UNWIND $pools AS pool
        MERGE (a:TOKEN {address: pool.token0_address})
        MERGE (b:TOKEN {address: pool.token1_address})

        MERGE (a)-[p_ab:POOL {address: pool.pool_address}]->(b)
        SET p_ab.src_address = pool.token0_address,
            p_ab.tgt_address = pool.token1_address,
            p_ab.src_balance = pool.token0_balance,
            p_ab.tgt_balance = pool.token1_balance

        MERGE (b)-[p_ba:POOL {address: pool.pool_address}]->(a)
        SET p_ba.src_address = pool.token1_address,
            p_ba.tgt_address = pool.token0_address,
            p_ba.src_balance = pool.token1_balance,
            p_ba.tgt_balance = pool.token0_balance
        """

        await graph_client.execute_query(query, **params)

    tasks = []
    BATCH_SIZE = 500
    for i in range(0, len(results[Entity.POOL]), BATCH_SIZE):
        batch = results[Entity.POOL][i:i+BATCH_SIZE]
        task = asyncio.create_task(_process_batch(batch))
        tasks.append(task)

    await asyncio.gather(*tasks)

# ----------------------------------------------------------

async def pool_enrich_token_price(
    results: dict[str, list], graph_client: AsyncDriver, **kwargs
):
    async def _run(client: AsyncDriver, pools: list[dict]):
        params = {
            "pools": [
                {
                    "pool_address": pool["address"],
                    "token0_address": pool["token0_address"].lower(),
                    "token1_address": pool["token1_address"].lower(),
                    "token0_balance": str(pool["token0_balance"]),
                    "token1_balance": str(pool["token1_balance"]),
                }
                for pool in pools
            ],
            "usdt_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower()
        }

        query = """
            UNWIND $pools AS pool
            MATCH (start:TOKEN {address: pool.token0_address}), (end:TOKEN {address: $usdt_address})
            CALL {
                WITH start, end, pool
                MATCH p = (start)-[*BFS..5]->(end)
                RETURN relationships(p) AS edges
                LIMIT 1
            }
            RETURN pool.pool_address AS pool_address, edges
        """

        records, _, _ = await client.execute_query(query, **params)
        
        price_map = {}
        for record in records:
            pool_address = record["pool_address"]
            edges = record["edges"]

            if edges:
                price = 1.0
                for edge in edges:
                    try:
                        ratio = int(edge["src_balance"]) / int(edge["tgt_balance"])
                        price = price / ratio
                    except (ValueError, ZeroDivisionError) as e:
                        print(f"Error calculating price for pool {pool_address}: {e}")
                        price = None
                        break
                
                if price is not None:
                    price_map[pool_address] = price
        
        for pool in pools:
            if pool["address"] in price_map:
                pool["token0_usd_price"] = price_map[pool["address"]]
                try:
                    pool["token1_usd_price"] = price * (pool["token0_balance"] / pool["token1_balance"])
                except ZeroDivisionError:
                    pool["token1_usd_price"] = 0.0
            else:
                print(f"No path to USDT found for pool: {pool['address']}")

    tasks = []
    BATCH_SIZE = 200
    for i in range(0, len(results[Entity.POOL]), BATCH_SIZE):
        batch = results[Entity.POOL][i:i+BATCH_SIZE]
        task = asyncio.create_task(_run(graph_client, batch))
        tasks.append(task)

    await asyncio.gather(*tasks)