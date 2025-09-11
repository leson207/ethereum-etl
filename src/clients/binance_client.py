import asyncio

import httpx
import orjson

from src.clients.throttler import Throttler
from src.logger import logger
from src.services.cache_service import cache_service
from src.utils.common import serialize_dict_value


class BinanceClient:
    def __init__(self, url: str, max_retries: int = 5, backoff: float = 3):
        self.url = url

        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }
        self.client = httpx.AsyncClient(
            headers=headers, http2=True, verify=False, timeout=timeout
        )
        self.throttler = Throttler(rate_limit=5, period=1)

        self.max_retries = max_retries
        self.backoff = backoff
        self._lock = asyncio.Lock()
        self._backoff_event = asyncio.Event()
        self._backoff_event.set()

    async def agg_trades(
        self,
        symbol: str,
        from_id: int = None,
        start_time: int = None,
        end_time: int = None,
        limit: int = None,
    ):
        params = {
            "symbol": symbol,
            "fromId": from_id,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }
        params = {k: v for k, v in params.items() if v is not None}
        key = "binance_agg_trades" + serialize_dict_value(params)
        cached = cache_service.get(key)
        if cached:
            return orjson.loads(cached)

        response = await self.retry(self._get_price, "/aggTrades", params)
        cache_service.set(key, orjson.dumps(response).decode("utf-8"))
        return response

    async def _get_price(self, path, params):
        async with self.throttler:
            response = await self.client.get(self.url + path, params=params)
            response = orjson.loads(response.content)
        
        if "code" in response:
            raise Exception(f"Binance error: {response['msg']}")
        
        return response

    async def retry(self, func, path, params):
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
