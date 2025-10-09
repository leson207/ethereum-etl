import asyncio
import itertools

import httpx
import orjson

from src.clients.throttler import Throttler
from src.configs.environment import env
from src.logger import logger


class RpcClient:
    def __init__(
        self,
        uris: list[str] = env.PROVIDER_URIS,
        max_retries: int = 5,
        backoff: float = 3,
    ):
        self.uris = uris
        timeout = httpx.Timeout(timeout=60)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
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

    def pick_uri(self, attempt: int):
        idx = (attempt - 1) % len(self.uris)
        return self.uris[idx]

    def form_request(self, method: str, params: list):
        request_id = next(self.request_counter)
        return {
            "id": request_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }

    async def get_web3_client_version(self):
        requests = [self.form_request("web3_clientVersion", [])]
        return await self.send_and_read(requests=requests)

    async def eth_call(self, param_sets: list[list]):
        requests = [self.form_request("eth_call", params) for params in param_sets]
        return await self.send_and_read(requests=requests)

    async def eth_get_balance(
        self, addresses: list[str], block_number: int | str = "latest"
    ):
        param_sets = [
            [
                addresses,
                hex(block_number) if isinstance(block_number, int) else block_number,
            ]
            for addresses in addresses
        ]
        requests = [
            self.form_request("eth_getBalance", params) for params in param_sets
        ]
        return await self.send_and_read(requests=requests)

    async def get_block_by_number(
        self, block_numbers: list[int], include_transaction: bool
    ):
        param_sets = [
            [hex(block_number), include_transaction] for block_number in block_numbers
        ]
        requests = [
            self.form_request("eth_getBlockByNumber", params) for params in param_sets
        ]
        return await self.send_and_read(requests=requests)

    async def get_receipt_by_block_number(self, block_numbers: list[int]):
        param_sets = [[hex(block_number)] for block_number in block_numbers]
        requests = [
            self.form_request("eth_getBlockReceipts", params) for params in param_sets
        ]
        return await self.send_and_read(requests=requests)

    async def get_trace_by_block_number(self, block_numbers: list[int]):
        param_sets = [[hex(block_number)] for block_number in block_numbers]
        requests = [self.form_request("trace_block", params) for params in param_sets]
        return await self.send_and_read(requests=requests)

    async def send_and_read(self, requests):
        responses = await self.post(requests)
        return responses

    async def post(self, requests: str):
        payload = orjson.dumps(requests)
        # network error retry
        for attempt in range(1, self.max_retries + 1):
            await self._backoff_event.wait()

            try:
                responses = await self._post(payload)
                responses = orjson.loads(
                    responses.content
                )  # b'<html><body><h1>429 Too Many Requests</h1>\nYou have sent too many requests in a given amount of time.\n</body></html>\n'
                responses = sorted(responses, key=lambda response: response["id"])
                logger.debug(f"Successfully processed {len(requests)} requests")
                return responses
            except Exception as e:
                logger.warning(
                    f"[Attempt {attempt}/{self.max_retries}] Failed to process {len(requests)} requests: {e}"
                )
                if self._backoff_event.is_set():
                    async with self._lock:
                        if self._backoff_event.is_set():  # Double-checked locking (safe in Python because of GIL) https://en.wikipedia.org/wiki/Double-checked_locking
                            self._backoff_event.clear()
                            logger.debug(f"⏳ Global backoff {self.backoff}s...")
                            await asyncio.sleep(self.backoff)
                            self._backoff_event.set()

        logger.error(
            f"Giving up on requests after {self.max_retries} attempts: {len(requests)} request"
        )
        return None

    async def _post(self, payload: str, attempt: int = 1):
        async with self.throttler:
            responses = await self.client.post(self.pick_uri(attempt), content=payload)

        return responses

    async def close(self):
        await self.client.aclose()
