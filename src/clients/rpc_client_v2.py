import asyncio
import itertools

import httpx
import orjson

from src.clients.throttler import Throttler
from src.logger import logger


class RpcClient:
    def __init__(self, uri, max_retries: int = 5, backoff: float = 3):
        self.uri = uri
        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }
        self.client = httpx.AsyncClient(
            headers=headers, http2=True, verify=False, timeout=timeout
        )
        self.request_counter = itertools.count()
        self.throttler = Throttler(rate_limit=50, period=1)

        self.max_retries = max_retries
        self.backoff = backoff
        self._lock = asyncio.Lock()
        self._backoff_event = asyncio.Event()
        self._backoff_event.set()

    def form_request(self, method, params):
        request_id = next(self.request_counter)
        return {
            "id": request_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }

    async def eth_call(self, param_sets):
        requests = [self.form_request("eth_call", params) for params in param_sets]
        payload = orjson.dumps(requests)
        responses = await self.post(payload)

        responses = orjson.loads(responses.content)
        return sorted(responses, key=lambda response: response["id"])

    async def get_blocks_by_number(self, block_numbers, include_transaction):
        param_sets = [
            [hex(block_number), include_transaction] for block_number in block_numbers
        ]
        requests = [
            self.form_request("eth_getBlockByNumber", params) for params in param_sets
        ]
        payload = orjson.dumps(requests)
        responses = await self.post(payload)

        responses = orjson.loads(responses.content)
        return sorted(responses, key=lambda response: response["id"])

    async def get_receipts_by_block_number(self, block_numbers):
        param_sets = [[hex(block_number)] for block_number in block_numbers]
        requests = [
            self.form_request("eth_getBlockReceipts", params) for params in param_sets
        ]
        payload = orjson.dumps(requests)
        responses = await self.post(payload)

        responses = orjson.loads(responses.content)
        return sorted(responses, key=lambda response: response["id"])

    async def get_traces_by_block_number(self, block_numbers, include_transaction):
        param_sets = [
            [hex(block_number), include_transaction] for block_number in block_numbers
        ]
        requests = [self.form_request("trace_block", params) for params in param_sets]
        payload = orjson.dumps(requests)
        responses = await self.post(payload)

        responses = orjson.loads(responses.content)
        return sorted(responses, key=lambda response: response["id"])

    async def post(self, payload):
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()

            try:
                responses = await self._post(payload)
                logger.debug(f"Successfully processed {len(payload)} requests")
                return responses
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

    async def _post(self, payload):
        async with self.throttler:
            responses = await self.client.post(self.uri, content=payload)

        return responses

    async def close(self):
        await self.client.aclose()
