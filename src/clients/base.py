import asyncio
from typing import Callable

import httpx

from src.logger import logger


# Nesscessary atttribute and sample retry method
class BaseClient:
    def __init__(self, url: str, throttler, max_retries: int = 5, backoff: float = 3):
        self.url = url
        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }
        self.client = httpx.AsyncClient(
            headers=headers, http2=True, verify=False, timeout=timeout
        )
        self.throttler = throttler

        self.max_retries = max_retries
        self.backoff = backoff
        self._lock = asyncio.Lock()
        self._backoff_event = asyncio.Event()
        self._backoff_event.set()

    async def _get_retry(self, func: Callable, path: str, params: list[dict]):
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()
            try:
                response = await func(path, params)
                logger.debug(f"Successfully processed 1 request - Params: {params}")
                return response
            except Exception as e:
                logger.warning(
                    f"[Attempt {attempt}/{self.max_retries}] Failed to process request - Params: {params} - {e}"
                )
                if self._backoff_event.is_set():
                    async with self._lock:
                        if self._backoff_event.is_set():  # Double-checked locking (safe in Python because of GIL) https://en.wikipedia.org/wiki/Double-checked_locking
                            self._backoff_event.clear()
                            logger.debug(f"⏳ Global backoff {self.backoff}s...")
                            await asyncio.sleep(self.backoff)
                            self._backoff_event.set()

        logger.error(
            f"Giving up on requests after {self.max_retries} attempts: 1 request - Params: {params}"
        )
        return None

    async def _post_retry(self, requests: list[dict]):
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
