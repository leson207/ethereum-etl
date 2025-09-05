import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.fetchers.base import BaseFetcher
from src.fetchers.json_client import JsonClient
from src.fetchers.throttler import Throttler
from src.schemas.python.contract import Contract
from src.utils.enumeration import EntityType


class ContractFetcher(BaseFetcher):
    def __init__(self, exporter):
        client = JsonClient(Throttler(rate_limit=5, period=1))
        super().__init__(client, exporter)

    def _from_request(self, contract_addresses):
        url = "https://api.etherscan.io/v2/api"
        requests = []
        for contract_address in contract_addresses:
            params = {
                "chainid": 1,
                "module": "contract",
                "action": "getsourcecode",  # getsourcecode, getabi
                "address": contract_address,
                "apikey": "2VY1QY3DNZDXDEMSCZFCW6HDVW3A3TEVFF",
            }

            requests.append((url, params))

        return requests

    async def run(
        self,
        items,
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        requests = self._from_request(items)

        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                description="Contract: ",
                total=(total or len(requests)),
                completed=(initial or 0),
            )

            tasks = [
                asyncio.create_task(
                    self._run(progress, task, requests[i : i + batch_size])
                )
                for i in range(0, len(requests), batch_size)
            ]

            for coro in asyncio.as_completed(tasks):
                result = await coro
                if result is None:
                    continue

                self.exporter.add_item(EntityType.CONTRACT_ADDRESS, result.model_dump())

    async def fetch(self, requests):
        response = await self.client.get(url=requests[0][0], params=requests[0][1])

        if response["status"] == "0":
            if response["result"] == "Invalid Address format":
                return None

            raise Exception(
                f"Params: {requests[0][1]} | Etherscan error: {response['result']}"
            )

        result = response["result"][0]

        contract = Contract(
            name=result["ContractName"],
            address=requests[0][1]["address"],
            abi=result["ABI"],
        )

        return contract
