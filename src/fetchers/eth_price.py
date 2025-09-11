import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.clients.binance_client import BinanceClient


class EthPriceFetcher:
    def __init__(self, client: BinanceClient):
        self.client = client

    async def run(
        self,
        timestamps: list[int],
        initial: int = None,
        total: int = None,
        batch_size: int = 1,
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
            length = len(timestamps)
            task_id = progress.add_task(
                description="ETH price: ",
                total=(total or length),
                completed=(initial or 0),
            )

            tasks = []
            results = [None] * len(timestamps)
            for idx, timestamp in enumerate(timestamps):
                atask = asyncio.create_task(
                    self._run(progress, task_id, results, timestamp, idx)
                )
                tasks.append(atask)

            for coro in asyncio.as_completed(tasks):
                await coro

            return results

    async def _run(
        self,
        progress: Progress,
        task_id: TaskID,
        storage: list,
        timestamp: int,
        position: int,
    ):
        response = await self.client.agg_trades(
            symbol="ETHUSDT", start_time=timestamp, limit=1
        )
        storage[position] = response[0]
        progress.update(task_id, advance=1)
