import asyncio

import httpx
import orjson
from fake_useragent import UserAgent
from tqdm.asyncio import tqdm_asyncio

from src.fetchers.throttler import Throttler
from src.logger import logger
from src.repositories.base import BaseRepository


class BaseFetcher:
    def __init__(self, client=None, repo: BaseRepository = None, pool_size: int = 10):
        self.client = client or self.create_client()
        self.repo = repo
        self.semaphore = asyncio.Semaphore(pool_size)
        self.throttler = Throttler(rate_limit=300 - pool_size, period=60, padding=0)

    def create_client(self):
        ua = UserAgent()
        headers = {"User-Agent": ua.random, "Accept": "application/json"}
        client = httpx.AsyncClient(
            headers=headers, timeout=httpx.Timeout(5.0, connect=3.0)
        )
        return client

    async def sem_task(self, func, args):
        async with self.semaphore:
            async with self.throttler:
                return await func(*args)

    async def run(
        self,
        items,
        initial=None,
        total=None,
        batch_size=30,
        extract_existing=False,
        return_existing=False,
    ):
        if not extract_existing and return_existing:
            data = self.repo.get_existing(items)
            for row in data:
                yield row

        if not extract_existing:
            items = self.repo.filter_existing(items)

        if not items:
            return

        generator = self.process(items, initial, total, batch_size)

        async for res in generator:
            yield res

    async def process(self, items, initial, total, batch_size):
        tasks = []
        for item in items:
            task = asyncio.create_task(self.sem_task(self._run, (item,)))
            tasks.append(task)

        for coro in tqdm_asyncio.as_completed(
            tasks,
            initial=(initial or 0) // batch_size,
            total=(total or len(items)) // batch_size,
            desc="Processing: ",
        ):
            result = await coro
            if result is None:
                continue
            yield result

    async def _process(self, item):
        max_retries = 3

        for attempt in range(1, max_retries + 1):
            try:
                res = await self.extract(item)
                logger.info(f"Successfully processed item: {item}")
                return res
            except Exception as e:
                logger.warning(
                    f"[Attempt {attempt}/{max_retries}] Failed to process item {item}: {e}"
                )
                await asyncio.sleep(1)

        logger.error(f"Giving up on item after {max_retries} attempts: {item}")
        return None

    async def extract(self, item):
        response = await self.client.get(item)
        data = orjson.loads(response.content)
        return data

    async def close(self):
        await self.client.aclose()
