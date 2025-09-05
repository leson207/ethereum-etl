import asyncio

from src.exporters.manager import ExportManager
from src.fetchers.json_client import JsonClient
from src.logger import logger


class BaseFetcher:
    def __init__(
        self,
        client: JsonClient,
        exporter: ExportManager,
        max_retries: int = 5,
        backoff: float = 3,
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
        show_progress=True,
    ):
        pass

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
