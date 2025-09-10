import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.clients.binance_client import BinanceClient
from src.schemas.python.eth_price import EthPrice
from src.utils.enumeration import EntityType


class EthPriceExtractor:
    def __init__(self, exporter, client: BinanceClient):
        self.exporter = exporter
        self.client = client

    async def run(
        self,
        timestamps,
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                description="ETH price: ",
                total=(total or len(timestamps)),
                completed=(initial or 0),
            )

            tasks = []
            for timestamp in timestamps:
                atask = asyncio.create_task(self._run(progress, task, timestamp, 1))
                tasks.append(atask)

            for coro in asyncio.as_completed(tasks):
                result = await coro
                if result is None:
                    continue
                self.exporter.add_item(EntityType.ETH_PRICE, result.model_dump())

    async def _run(self, progress, task, input, input_size):
        response = await self.client.get_price(symbol="ETHUSDT", timestamp=input)
        res = self.extract(input, response)
        progress.update(task, advance=input_size)
        return res

    def extract(self, input, response):
        eth_price = EthPrice(timestamp=input, price=float(response["p"]))

        return eth_price
