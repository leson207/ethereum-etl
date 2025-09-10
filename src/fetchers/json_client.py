import httpx
import orjson

from src.fetchers.throttler import Throttler
from src.services.cache_service import cache_service
from src.utils.common import dump_json

class JsonClient:
    def __init__(self, throttler: Throttler):
        self.throttler = throttler
        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "web3.py/7.13.0/web3.providers.rpc.async_rpc.AsyncHTTPProvider",
        }
        self.client = httpx.AsyncClient(
            headers=headers, http2=True, verify=False, timeout=timeout
        )

    async def get(self, url, params, cache_key=None):
        if cache_key:
            cached_value = cache_service.get(cache_key)
            if cached_value:
                return orjson.loads(cached_value)

        async with self.throttler:
            response = await self.client.get(url, params=params)
            value = response.content

        if cache_key:
            cache_service.set(cache_key, value)

        return orjson.loads(value)

    async def post(self, uri, payload, cache_key=None):
        if cache_key:
            cached_value = cache_service.get(cache_key)
            if cached_value:
                return orjson.loads(cached_value)

        encoded = orjson.dumps(payload)
        async with self.throttler:
            responses = await self.client.post(uri, content=encoded)
            value = responses.content # byte

        if cache_key:
            cache_service.set(cache_key, value)

        return orjson.loads(value)

    async def close(self):
        await self.client.aclose()
