import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from clients.rpc_client import RpcClient
from src.schemas.python.raw_receipt import RawReceipt
from src.utils.enumeration import EntityType


class RawReceiptExtractor:
    def __init__(self, exporter, client: RpcClient):
        self.exporter = exporter
        self.client = client

    async def run(
        self,
        block_numbers,
        initial=None,
        total=None,
        batch_size=30,
        show_progress=True,
    ):
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                description="Raw Receipt: ",
                total=(total or len(block_numbers)),
                completed=(initial or 0),
            )

            tasks = []
            for i in range(0, len(block_numbers), batch_size):
                batch = block_numbers[i : i + batch_size]
                atask = asyncio.create_task(
                    self._run(progress, task, batch, len(batch))
                )
                tasks.append(atask)

            for coro in asyncio.as_completed(tasks):
                result = await coro
                if result is None:
                    continue

                result = [i.model_dump() for i in result]
                self.exporter.add_items(EntityType.RAW_RECEIPT, result)

    async def _run(self, progress, task, input, input_size):
        responses = await self.client.get_receipt_by_block_number(block_numbers=input)
        res = self.extract(input, responses)
        progress.update(task, advance=input_size)
        return res

    def extract(self, inputs, responses):
        raws = []
        for input, response in zip(inputs, responses):
            raw = RawReceipt(
                block_number=input,
                data=response["result"],
            )
            raws.append(raw)

        return raws
