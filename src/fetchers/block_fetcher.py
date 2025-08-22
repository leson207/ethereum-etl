import asyncio

from tqdm.asyncio import tqdm_asyncio

from src.fetchers.base import BaseFetcher
from src.logger import logger
from src.schemas.python.raw_block import RawBlock
from src.utils.enumeration import EntityType
from src.utils.progress_bar import get_progress_bar


class BlockFetcher(BaseFetcher):
    async def run(
        self,
        items,
        initial=None,
        total=None,
        batch_size=30,
        show_progress=True,
    ):
        requests = [
            (
                "eth_getBlockByNumber",
                [hex(item["block_number"]), item["included_transaction"]],
            )
            for item in items
        ]

        tasks = []
        for i in range(0, len(requests), batch_size):
            batch = requests[i : i + batch_size]
            task = asyncio.create_task(self._process(batch))
            tasks.append(task)

        p_bar = get_progress_bar(
            tqdm_asyncio,
            tasks,
            initial=(initial or 0) // batch_size,
            total=(total or len(requests)) // batch_size,
            show=show_progress,
        )

        for coro in p_bar:
            result = await coro
            if result is None:
                continue

            result = [i.model_dump() for i in result]
            self.exporter.add_item(EntityType.RAW_BLOCK, result)

    async def _process(self, requests):
        max_retries = 3

        for attempt in range(1, max_retries + 1):
            try:
                raws = await self.fetch(requests)
                logger.debug(f"Successfully processed {len(requests)} requests")
                return raws
            except Exception as e:
                logger.warning(
                    f"[Attempt {attempt}/{max_retries}] Failed to process requests {requests}: {e}"
                )
                await asyncio.sleep(1)

        logger.error(f"Giving up on requests after {max_retries} attempts: {requests}")
        return None

    async def fetch(self, requests):
        response = await self.client.make_batch_request(requests)

        raws = []
        for request, data in zip(requests, response):
            raw = RawBlock(
                block_number=int(request[1][0], 16),
                included_transaction=request[1][1],
                data=data["result"],
            )
            raws.append(raw)

        return raws
