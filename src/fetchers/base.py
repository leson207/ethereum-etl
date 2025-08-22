import asyncio

import orjson
from tqdm.asyncio import tqdm_asyncio

from src.logger import logger
from src.utils.progress_bar import get_progress_bar


class BaseFetcher:
    def __init__(self, client, exporter=None):
        self.client = client
        self.exporter = exporter

    async def run(
        self, items, initial=None, total=None, batch_size=1, show_progress=True
    ):
        tasks = []
        for item in items:
            task = asyncio.create_task(self._process(item))
            tasks.append(task)

        p_bar = get_progress_bar(
            tqdm_asyncio,
            tasks,
            initial=(initial or 0) // batch_size,
            total=(total or len(tasks)) // batch_size,
            show=show_progress,
        )

        for coro in p_bar:
            result = await coro
            if result is None:
                continue
            yield result

    async def process(self, items, initial, total, batch_size):
        tasks = []
        for item in items:
            task = asyncio.create_task(self._process(item))
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
