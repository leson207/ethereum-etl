import asyncio
from types import SimpleNamespace

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
from src.utils.enumeration import EntityType


class PriceFetcher(BaseFetcher):
    def __init__(self, exporter):
        client = JsonClient(Throttler(rate_limit=50, period=1))
        super().__init__(client, exporter)

    def _from_request(self, timestamps):
        url = "https://api4.binance.com/api/v3/aggTrades"
        # GET /api/v3/avgPrice # use this if time too close to current - realtime
        # GET /api/v3/ticker/price # use this if time too close to current- realtime



        requests = []
        for timestamp in timestamps:
            params = {"symbol": "ETHUSDT", "startTime": timestamp, "limit": 1}

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
                description="Price: ",
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
                
                print(result)
                # self.exporter.add_item(EntityType.CONTRACT_ADDRESS, result.model_dump())

    async def fetch(self, requests):
        response = await self.client.get(url=requests[0][0], params=requests[0][1])
        print(response)
        price = SimpleNamespace(
            timestamp=requests[0][1]["startTime"], price=float(response[0]["p"])
        )

        return price
