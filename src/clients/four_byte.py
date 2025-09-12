import asyncio

import httpx
import orjson

from src.clients.throttler import Throttler
from src.logger import logger
from typing import Callable
from src.services.cache_service import cache_service


class FourByteClient:
    def __init__(self, url: str, max_retries: int = 5, backoff: float = 3):
        self.url = url

        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }
        self.client = httpx.AsyncClient(
            headers=headers,
            http2=True,
            verify=False,
            timeout=timeout,
        )
        self.throttler = Throttler(rate_limit=5, period=1)

        self.max_retries = max_retries
        self.backoff = backoff
        self._lock = asyncio.Lock()
        self._backoff_event = asyncio.Event()
        self._backoff_event.set()

    async def get_signature_from_hex(self, hex_signature: str, use_cached: bool = True):
        params = {"hex_signature": hex_signature}

        key = f"4bytes_event_signatures_{params['hex_signature']}"
        if use_cached:
            result = cache_service.get(key)
            if result:
                return orjson.loads(result)

        response = await self.retry(
            self._get_event_signature, "/event-signatures/", params
        )
        result = response["results"][0]
        cache_service.set(key, orjson.dumps(result).decode("utf-8"))

        # {
        #     "count": 1,
        #     "next": null,
        #     "previous": null,
        #     "results": [
        #         {
        #         "id": 1,
        #         "created_at": "2020-11-30T22:38:00.801049Z",
        #         "text_signature": "Transfer(address,address,uint256)",
        #         "hex_signature": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        #         "bytes_signature": "ÝòR­\u001bâÈÂ°hü7ª+§ñcÄ¡\u0016(õZMõ#³ï"
        #         }
        #     ]
        # }

        return result

    async def _get_event_signature(self, path: str, params: dict):
        async with self.throttler:
            response = await self.client.get(self.url + path, params=params)

        response = orjson.loads(response.content)
        return response

    async def retry(self, func: Callable, path: str, params: dict):
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

    async def close(self):
        await self.client.aclose()
