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
from src.schemas.python.raw_trace import RawTrace
from src.utils.enumeration import EntityType


class RawTraceFetcher(BaseFetcher):
    def _from_request(self, params):
        requests = [
            (
                "trace_block",
                [hex(param["block_number"])],
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
                description="Raw Trace: ",
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

                result = [i.model_dump() for i in result]
                self.exporter.add_items(EntityType.RAW_TRACE, result)

    async def fetch(self, requests):
        response = await self.client.send_batch_request(requests)

        raws = []
        for request, data in zip(requests, response):
            raw = RawTrace(
                block_number=int(request[1][0], 16),
                data=data["result"],
            )
            raws.append(raw)

        return raws
