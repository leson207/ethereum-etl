import asyncio
import itertools

from src.clients.json_client import JsonClient
from src.clients.throttler import Throttler
from src.logger import logger


class GetClient(JsonClient):
    def __init__(self, uri, max_retries: int = 5, backoff: float = 3):
        super().__init__(Throttler(rate_limit=50, period=1))

        self.uri = uri

        self.max_retries = max_retries

        self.backoff = backoff
        self._lock = asyncio.Lock()
        self._backoff_event = asyncio.Event()
        self._backoff_event.set()

    async def send(self, requests):
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()

            try:
                raws = await self._send(requests)
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

    async def _send(self, url, params):
        response = await self.client.get(url=url, params=params)

    async def close(self):
        await self.client.aclose()
