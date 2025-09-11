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

from src.clients.etherscan_client import EtherscanClient


class ContractFetcher:
    def __init__(self, client: EtherscanClient):
        self.client = client

    async def run(
        self,
        contract_addresses: list[str],
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
            length = len(contract_addresses)
            task_id = progress.add_task(
                description="Contract: ",
                total=(total or length),
                completed=(initial or 0),
            )

            tasks = []
            results = [None] * length
            for idx, contract_address in enumerate(contract_addresses):
                atask = asyncio.create_task(
                    self._run(progress, task_id, results, contract_address, idx)
                )
                tasks.append(atask)

            for coro in asyncio.as_completed(tasks):
                result = await coro
                if result is None:
                    continue

            return results

    async def _run(
        self,
        progress: Progress,
        task_id: TaskID,
        storage: list,
        contract_address: str,
        position: int,
    ):
        response = await self.client.get_contract_source_code(contract_address)
        storage[position] = response
        progress.update(task_id, advance=1)
