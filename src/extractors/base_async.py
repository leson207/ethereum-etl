import asyncio

from src.exporters.manager import ExportManager
from src.fetchers.json_client import JsonClient
from src.logger import logger


class AsyncProcessor:
    async def run(
        self,
        items,
        initial=None,
        total=None,
        batch_size=30,
        show_progress=True
    ):
        pass

    async def _run(self, progress, task, input, input_size):
        res = await self.process(input)
        progress.update(task, advance=input_size)
        return res

    async def process(self, input, input_size):
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()

            try:
                raws = await self._process(input)
                logger.debug(f"Successfully processed {input_size} items")
                return raws
            except Exception as e:
                logger.warning(
                    f"[Attempt {attempt}/{self.max_retries}] Failed to process {input_size} items: {e}"
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

    async def _process(self, requests):
        pass
