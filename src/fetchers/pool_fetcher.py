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
from types import SimpleNamespace

class PoolFetcher(BaseFetcher):
    def _from_request(self, params):
        requests = [
            (
                "eth_call",
                [hex(param["to"]), param["data"]],
            )
            for param in params
        ]
        return requests

    async def run(
        self,
        params,
        initial=None,
        total=None,
        batch_size=30,
        show_progress=True,
    ):
        requests = self._from_request(params)

        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                description="Pool contract: ",
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

                

    async def fetch(self, requests):
        response = await self.client.send_batch_request(requests)
        if "error" in response:
            # {'jsonrpc': '2.0', 'id': 0, 'error': {'code': 3, 'message': 'execution reverted'}}
            return None

        raws = []
        for request, data in zip(requests, response):
            raw = SimpleNamespace(
                pool_address = request[0]
            )
            raws.append(raw)

        return raws
