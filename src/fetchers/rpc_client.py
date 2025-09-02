import itertools

import httpx
import orjson

from src.fetchers.throttler import Throttler


class RPCClient:
    def __init__(self, uri):
        self.uri = uri
        self.request_counter = itertools.count()
        self.throttler = Throttler(rate_limit=50, period=1)

        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "web3.py/7.13.0/web3.providers.rpc.async_rpc.AsyncHTTPProvider",
        }
        self.client = httpx.AsyncClient(
            headers=headers, http2=True, verify=False, timeout=timeout
        )
    
    async def send_single_request(self, data: tuple[str, list]):
        method, params = data
        payload = self.form_request(method, params)
        encoded = orjson.dumps(payload)

        async with self.throttler:
            responses = await self.client.post(self.uri, content=encoded)

        return orjson.loads(responses.content)

    async def send_batch_request(self, data: list[tuple[str, list]]):
        payload = [
            self.form_request(method, params) for method, params in data
        ]
        encoded = orjson.dumps(payload)

        async with self.throttler:
            responses = await self.client.post(self.uri, content=encoded)

        return sorted(
            orjson.loads(responses.content), key=lambda response: response["id"]
        )

    def form_request(self, method, params):
        request_id = next(self.request_counter)
        return {
            "id": request_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }

    async def close(self):
        await self.client.aclose()
