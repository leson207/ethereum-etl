import asyncio

import httpx
import orjson

from src.clients.throttler import Throttler
from src.logger import logger


class JsonClient:
    def __init__(self, throttler: Throttler, max_retries: int = 5, backoff: float = 3):
        self.throttler = throttler
        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }
        self.client = httpx.AsyncClient(
            headers=headers, http2=True, verify=False, timeout=timeout
        )

        self.max_retries = max_retries

        self.backoff = backoff
        self._lock = asyncio.Lock()
        self._backoff_event = asyncio.Event()
        self._backoff_event.set()

    async def _get(self, url, params):
        async with self.throttler:
            response = await self.client.get(url, params=params)

        return orjson.loads(response.content)

    async def post(self, uri, payload):
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()

            try:
                raws = await self._post(uri, payload)
                logger.debug(f"Successfully processed {len(payload)} requests")
                return raws
            except Exception as e:
                logger.warning(
                    f"[Attempt {attempt}/{self.max_retries}] Failed to process {len(payload)} requests: {e}"
                )
                if self._backoff_event.is_set():
                    async with self._lock:
                        if self._backoff_event.is_set():  # Double-checked locking (safe in Python because of GIL) https://en.wikipedia.org/wiki/Double-checked_locking
                            self._backoff_event.clear()
                            logger.debug(f"⏳ Global backoff {self.backoff}s...")
                            await asyncio.sleep(self.backoff)
                            self._backoff_event.set()

        logger.error(
            f"Giving up on requests after {self.max_retries} attempts: {len(payload)} request"
        )
        return None

    async def _post(self, uri, payload):
        encoded = orjson.dumps(payload)
        async with self.throttler:
            responses = await self.client.post(uri, content=encoded)

        return orjson.loads(responses.content)
        # try:
        #     tmp = orjson.loads(responses.content)
        #     print('8'*100)
        #     return tmp
        # except:
        #     print(responses.content)
        #     raise

    async def close(self):
        await self.client.aclose()
