import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.clients.rpc_client import RpcClient


class RawReceiptFetcher:
    def __init__(self, client: RpcClient):
        self.client = client

    async def run(
        self,
        block_numbers: list[int],
        initial: int = None,
        total: int = None,
        batch_size: int = 30,
        show_progress: bool = True,
    ):
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            length = len(block_numbers)
            task_id = progress.add_task(
                description="Raw Receipt: ",
                total=(total or length),
                completed=(initial or 0),
            )

            tasks = []
            results = [None] * length
            for i in range(0, length, batch_size):
                atask = asyncio.create_task(
                    self._run(
                        progress,
                        task_id,
                        results,
                        block_numbers[i : i + batch_size],
                        range(i, min(i + batch_size, length)),
                    )
                )
                tasks.append(atask)

            for coro in asyncio.as_completed(tasks):
                await coro

            return results

    async def _run(
        self,
        progress: Progress,
        task_id: TaskID,
        storage: list,
        block_numbers: list[int],
        positions: list[int],
    ):
        responses = await self.client.get_receipt_by_block_number(
            block_numbers=block_numbers
        )
        for position, response in zip(positions, responses):
            storage[position] = response["result"]

        progress.update(task_id, advance=len(positions))
