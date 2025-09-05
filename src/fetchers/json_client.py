import httpx
import orjson

from src.fetchers.throttler import Throttler


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

    async def get(self, url, params):
        async with self.throttler:
            response = await self.client.get(url, params=params)

        return orjson.loads(response.content)

    async def post(self, uri, payload):
        encoded = orjson.dumps(payload)

        async with self.throttler:
            responses = await self.client.post(uri, content=encoded)

        return orjson.loads(responses.content)

    async def close(self):
        await self.client.aclose()
