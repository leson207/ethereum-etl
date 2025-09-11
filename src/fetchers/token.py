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

from src.abis.function import FUNCTION_HEX_SIGNATURES
from src.clients.rpc_client import RpcClient


class TokenFetcher:
    def __init__(self, client: RpcClient):
        self.client = client

    async def run(
        self,
        contract_addresses: list[str],
        initial: int = None,
        total: int = None,
        batch_size: int = 5,
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
                description="Token: ",
                total=(total or length),
                completed=(initial or 0),
            )

            tasks = []
            results = [None] * length
            for i in range(0, length, batch_size):
                batch = contract_addresses[i : i + batch_size]

                atask = asyncio.create_task(
                    self._run(
                        progress,
                        task_id,
                        results,
                        batch,
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
        contract_addresses: list[str],
        positions: list[int],
    ):
        param_sets = self._form_param_set(contract_addresses)
        responses = await self.client.eth_call(param_sets=param_sets)

        set_size = 4
        for idx, pos in enumerate(positions):
            start = idx * set_size
            end = start + set_size
            storage[pos] = responses[start:end]

        progress.update(task_id, advance=len(positions))

    def _form_param_set(self, contract_addresses):
        param_sets = []
        for contract_address in contract_addresses:
            for func in ["name", "symbol", "decimals", "totalSupply"]:
                param_set = [
                    {
                        "to": "0x" + contract_address.lower()[-40:],
                        "data": FUNCTION_HEX_SIGNATURES["erc20"][func],
                    }
                ]
                param_sets.append(param_set)

        return param_sets
