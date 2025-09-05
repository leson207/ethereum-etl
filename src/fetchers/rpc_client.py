import itertools

from src.fetchers.json_client import JsonClient
from src.fetchers.throttler import Throttler


class RPCClient(JsonClient):
    def __init__(self, uri):
        super().__init__(Throttler(rate_limit=50, period=1))

        self.uri = uri
        self.request_counter = itertools.count()

    async def send_single_request(self, method: str, params: list):
        payload = self.form_request(method, params)
        response = await self.post(self.uri, payload=payload)

        return response

    async def send_batch_request(self, data: list[tuple[str, list]]):
        payload = [self.form_request(method, params) for method, params in data]

        responses = await self.post(self.uri, payload=payload)

        return sorted(responses, key=lambda response: response["id"])

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
