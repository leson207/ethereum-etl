import asyncio

from neo4j import AsyncDriver

from src.abis.function import FUNCTION_HEX_SIGNATURES
from src.clients.rpc_client import RpcClient
from src.logger import logger
from src.utils.enumeration import Entity


USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower()
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
WETH_USDT_UNISWAP_V2_ADDRESS = "0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852".lower()


def pool_init_address(results: dict[str, list], **kwargs):
    addresses = [event["pool_address"] for event in results[Entity.EVENT]]
    addresses = list(set(addresses))
    results[Entity.POOL] = [{"address": address} for address in addresses]


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

            if token0["result"]=="0x" and token0["result"]=="0x": # the pool address actually a token address
                results[Entity.POOL].remove(pool)
                continue

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
    async def _run(pools: list[dict]):
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
            ]
        }

        query = """
            UNWIND $pools AS pool
            MERGE (t0:TOKEN {address: pool.token0_address})
            MERGE (t1:TOKEN {address: pool.token1_address})

            MERGE (t0)-[p_t0_t1:POOL {address: pool.pool_address}]->(t1)
            SET p_t0_t1.src_address = pool.token0_address,
                p_t0_t1.tgt_address = pool.token1_address,
                p_t0_t1.src_balance = pool.token0_balance,
                p_t0_t1.tgt_balance = pool.token1_balance

            MERGE (t1)-[p_t1_t0:POOL {address: pool.pool_address}]->(t0)
            SET p_t1_t0.src_address = pool.token1_address,
                p_t1_t0.tgt_address = pool.token0_address,
                p_t1_t0.src_balance = pool.token1_balance,
                p_t1_t0.tgt_balance = pool.token0_balance
        """

        await graph_client.execute_query(query, **params)

    tasks = []
    BATCH_SIZE = 500
    for i in range(0, len(results[Entity.POOL]), BATCH_SIZE):
        batch = results[Entity.POOL][i:i+BATCH_SIZE]
        task = asyncio.create_task(_run(batch))
        tasks.append(task)

    await asyncio.gather(*tasks)

# ----------------------------------------------------------

async def pool_enrich_token_price(
    results: dict[str, list], graph_client: AsyncDriver, **kwargs
):
    async def _run(client: AsyncDriver, pools: list[dict], token_decimals: dict[str,int], weth_usdt_ratio: float):
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
            "weth_address": WETH_ADDRESS
        }

        # this query do not preserve pools order
        query = """
            UNWIND $pools AS pool
            MATCH (start:TOKEN {address: pool.token0_address}), (end:TOKEN {address: $weth_address})
            CALL {
                WITH start, end, pool
                MATCH p = (start)-[*BFS..5]->(end)
                WHERE NONE(n IN nodes(p) WHERE n.decimals IS NULL)
                RETURN nodes(p) AS nodes, relationships(p) AS edges
                LIMIT 1
            }
            RETURN pool.pool_address AS pool_address, nodes, edges
        """

        # if there no path, the number of record != number of pool
        records, _, _ = await client.execute_query(query, **params)

        price_map = {}
        for record in records:
            pool_address = record["pool_address"]

            for node in record["nodes"]:
                token_decimals[node["address"]] = node["decimals"]
            
            price = 1.0            
            for edge in record["edges"]:
                try:
                    numerator = int(edge["src_balance"]) * 10**token_decimals[edge["tgt_address"]]
                    denominator = int(edge["tgt_balance"]) * 10**token_decimals[edge["src_address"]]
                    ratio = numerator / denominator
                    price = price / ratio
                except (ValueError, ZeroDivisionError) as e:
                    logger.info(f"Error calculating price for pool {pool_address}: {e}")
                    price = None
                    break
            
            if price is not None:
                price_map[pool_address] = price
        
        for pool in pools:
            if pool["token0_address"] == WETH_ADDRESS:
                price_map[pool["address"]] = 1.0

            if pool["address"] in price_map:
                pool["token0_usd_price"] = price_map[pool["address"]] / weth_usdt_ratio
                try:
                    numerator = pool["token0_balance"]* 10**token_decimals[pool["token1_address"]]
                    denominator = pool["token1_balance"]* 10**token_decimals[pool["token0_address"]]
                    ratio = numerator / denominator
                    pool["token1_usd_price"] = pool["token0_usd_price"] * ratio
                except ZeroDivisionError:
                    pool["token1_usd_price"] = 0.0
            else:
                logger.info(f"No path to USDT found for pool: {pool['address']}")
            
    async def get_ratio(client: AsyncDriver):
        query = """
            MATCH (weth:TOKEN {address: $weth_address})
            MATCH (usdt:TOKEN {address: $usdt_address})
            MATCH (weth)-[pool:POOL {address: $pool_address}]->(usdt)
            RETURN weth, usdt, pool;
        """
        records, _, _= await client.execute_query(
            query,
            weth_address=WETH_ADDRESS,
            usdt_address=USDT_ADDRESS,
            pool_address=WETH_USDT_UNISWAP_V2_ADDRESS
        )
        if not records:
            return 4000 # if there not path from weth to usdt, temporary use this value until get that pair
        numerator = int(records[0]["pool"]["src_balance"]) * 10**records[0]["usdt"]["decimals"]
        denominator = int(records[0]["pool"]["tgt_balance"]) * 10**records[0]["weth"]["decimals"]
        ratio = numerator / denominator
        return ratio

    weth_usdt_ratio = await get_ratio(graph_client)

    token_decimals = {
        token["address"]: token["decimals"]
        for token in results[Entity.TOKEN]
    }
    token_decimals[USDT_ADDRESS] = 6
    tasks = []
    BATCH_SIZE = 500
    for i in range(0, len(results[Entity.POOL]), BATCH_SIZE):
        batch = results[Entity.POOL][i:i+BATCH_SIZE]
        task = asyncio.create_task(_run(graph_client, batch, token_decimals, weth_usdt_ratio))
        tasks.append(task)

    await asyncio.gather(*tasks)