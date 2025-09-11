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

from src.clients.four_byte import FourByteClient


class EventSignatureFetcher:
    def __init__(self, client: FourByteClient):
        self.client = client

    async def run(
        self,
        hex_signatures: list[str],
        initial: int = None,
        total: int = None,
        batch_size: int = 1,
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
            length = len(hex_signatures)
            
            task_id = progress.add_task(
                description="Event signature: ",
                total=(total or length),
                completed=(initial or 0),
            )

            tasks = []
            results = [None] * length
            for idx, hex_signature in enumerate(hex_signatures):
                atask = asyncio.create_task(
                    self._run(progress, task_id, results, hex_signature, idx)
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
        hex_signature: str,
        position: int,
    ):
        response = await self.client.get_signature_from_hex(hex_signature=hex_signature)
        storage[position] = response
        progress.update(task_id, advance=1)
