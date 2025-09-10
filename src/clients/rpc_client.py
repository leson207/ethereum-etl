import itertools

from src.clients.json_client import JsonClient
from src.clients.throttler import Throttler


class RpcClient(JsonClient):
    def __init__(self, uri, max_retries: int = 5, backoff: float = 3):
        super().__init__(
            throttler=Throttler(rate_limit=50, period=1),
            max_retries=max_retries,
            backoff=backoff,
        )

        self.uri = uri
        self.request_counter = itertools.count()

    async def send(self, data: list[tuple[str, list]]):
        payload = [self.form_rpc_request(method, params) for method, params in data]

        responses = await self.post(self.uri, payload=payload)

        return sorted(responses, key=lambda response: response["id"])

    def form_rpc_request(self, method, params):
        request_id = next(self.request_counter)
        return {
            "id": request_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }

    async def close(self):
        await self.client.aclose()
