import asyncio
from typing import Callable

import httpx
import orjson

from src.clients.throttler import Throttler
from src.logger import logger
from src.services.cache_service import cache_service


# Error in response so each request function must have it corresponding retry
class EtherscanClient:
    def __init__(self, url: str, max_retries: int = 5, backoff: float = 3):
        self.url = url
        self.chain_id = 1
        self.api_key = "2VY1QY3DNZDXDEMSCZFCW6HDVW3A3TEVFF"

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

    async def get_contract_source_code(self, contract_address: str):
        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": contract_address,
        }

        key = f"etherscan_{params['module']}_{params['action']}_{params['address']}"
        result = cache_service.get(key)
        if result:
            return orjson.loads(result)

        response = await self.retry(self._get_contract_source_code, params)
        if response["result"] == "Invalid Address format":
            return result

        result = response["result"][0]
        cache_service.set(key, orjson.dumps(result).decode("utf-8"))
        return result

    async def _get_contract_source_code(self, params: dict):
        async with self.throttler:
            response = await self.client.get(self.url, params=params)
        response = orjson.loads(response.content)
        if response["status"] == "0":
            if response["result"] == "Invalid Address format":
                return response
            raise Exception(f"Etherscan error: {response['result']}")
        return response

    async def retry(self, func: Callable, params: dict):
        full_params = params | {"chainid": self.chain_id, "apikey": self.api_key}
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()
            try:
                response = await func(full_params)
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
