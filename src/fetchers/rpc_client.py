import itertools

import httpx
import orjson

import contextvars


class RPCClient:
    def __init__(self, uri):
        self.uri = uri
        self.request_counter = itertools.count()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "web3.py/7.13.0/web3.providers.rpc.async_rpc.AsyncHTTPProvider",
        }
        self.client = httpx.AsyncClient(headers=headers, http2=True, verify=False)
        # self._batching_context = contextvars.ContextVar("batching_context", default=None)

    async def make_request(self, method , params):
        payload = self.form_request(method, params)
        encoded = orjson.dumps(payload)
        responses = await self.client.post(self.uri, content=encoded)
        return orjson.loads(responses.content)

    async def make_batch_request(self, batch_requests: list[tuple[str, list]]):
        payload = [
            self.form_request(method, params) for method, params in batch_requests
        ]
        # orjson faster that json
        encoded = orjson.dumps(payload)
        responses = await self.client.post(self.uri, content=encoded)
        return sorted(
            orjson.loads(responses.content), key=lambda response: response["id"]
        )
        # TODO
        # if not isinstance(response, list):
        #         # RPC errors return only one response with the error object
        #         return response
        # [
        # {"id": 1, "jsonrpc": "2.0", "result": "0x123..."},
        # {"id": 2, "jsonrpc": "2.0", "error": {"code": -32602, "message": "Invalid params"}}
        # ]

#         Network-level issues (timeout, connection reset, 502, etc.) → retried.

# RPC-level errors (invalid opcode, bad method, contract revert, out-of-gas, etc.) → never retried, because they are deterministic errors returned by the node.

    def form_request(self, method, params=None):
        request_id = next(self.request_counter)
        return {
            "id": request_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params or [],
        }

    async def close(self):
        await self.client.aclose()
