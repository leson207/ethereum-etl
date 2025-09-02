import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.logger import logger


class BaseFetcher:
    def __init__(
        self, client, exporter: list = [], max_retries: int = 5, backoff: float = 3
    ):
        self.client = client
        self.exporter = exporter
        self.max_retries = max_retries

        self.backoff = backoff
        self._lock = asyncio.Lock()
        self._backoff_event = asyncio.Event()
        self._backoff_event.set()

    def _from_request(self, params: list[dict]):
        pass

    async def run(
        self,
        params,
        initial=None,
        total=None,
        batch_size=30,
        desc="Raw: ",
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
                description=desc,
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
                self.exporter.extend(result)

    async def _run(self, progress, task, requests):
        res = await self._process(requests)
        progress.update(task, advance=len(requests))
        return res

    # async def _process(self, requests):
    #     for attempt in range(1, self.max_retries + 1):
    #         try:
    #             raws = await self.fetch(requests)
    #             logger.debug(f"Successfully processed {len(requests)} requests")
    #             return raws
    #         except Exception as e:
    #             logger.warning(
    #                 f"[Attempt {attempt}/{self.max_retries}] Failed to process requests {requests}: {e}"
    #             )
    #             await asyncio.sleep(1)

    #     logger.error(
    #         f"Giving up on requests after {self.max_retries} attempts: {requests}"
    #     )
    #     return None

    async def _process(self, requests):
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()

            try:
                raws = await self.fetch(requests)
                logger.debug(f"Successfully processed {len(requests)} requests")
                return raws
            except Exception as e:
                logger.warning(
                    f"[Attempt {attempt}/{self.max_retries}] Failed to process {len(requests)} requests: {e}"
                )
                if self._backoff_event.is_set():
                    async with self._lock:
                        if self._backoff_event.is_set():  # Double-checked locking (safe in Python because of GIL) https://en.wikipedia.org/wiki/Double-checked_locking
                            self._backoff_event.clear()
                            logger.debug(f"⏳ Global backoff {self.backoff}s...")
                            await asyncio.sleep(self.backoff)
                            self._backoff_event.set()

        logger.error(
            f"Giving up on requests after {self.max_retries} attempts: {len(requests)} request"
        )
        return None

    async def fetch(self, requests):
        pass
