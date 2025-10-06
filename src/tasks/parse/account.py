from src.utils.enumeration import Entity
import asyncio
from src.clients.rpc_client import RpcClient
from src.utils.common import hex_to_dec


def parse_account(results: dict[str, list], **kwargs):
    account_addresses = []
    for receipt in results[Entity.RAW_RECEIPT]:
        item = receipt["data"]
        contract_addresses = []

        account_addresses.append(item.get("from"))
        if item["contractAddress"]:
            contract_addresses.append(item["contractAddress"])

        for log in item.get("logs", []):
            contract_addresses.append(log["address"])

        if item["to"] not in contract_addresses:
            account_addresses.append(item["to"])

    account_addresses = list(set(account_addresses))

    results[Entity.POOL] = [
        {"address": address} for address in account_addresses if address
    ]


# ---------------------------------------------


async def enrich_account_balance(
    results: dict[str, list], rpc_client: RpcClient, batch_size: int, **kwargs
):
    async def _run(rpc_client: RpcClient, accounts: list[dict]):
        addresses = [account["address"] for account in accounts]
        responses = await rpc_client.eth_get_balance(addresses)

        for account, response in zip(accounts, responses):
            if "error" in response:
                continue

            account["balance"] = hex_to_dec(response["result"])

    tasks = []
    for i in range(0, len(results[Entity.ACCOUNT]), batch_size):
        batch = results[Entity.ACCOUNT][i : i + batch_size]
        task = asyncio.create_task(_run(rpc_client, batch))
        tasks.append(task)

    for coro in asyncio.as_completed(tasks):
        await coro
