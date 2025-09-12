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


class BalanceFetcher:
    def __init__(self, client: RpcClient):
        self.client = client

    async def run(
        self,
        pools: list[dict],
        initial: int = None,
        total: int = None,
        batch_size: int = 10,
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
            length = len(pools)
            task_id = progress.add_task(
                description="Balance: ",
                total=(total or length),
                completed=(initial or 0),
            )

            tasks = []
            results = [None] * length
            for i in range(0, length, batch_size):
                batch = pools[i : i + batch_size]

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
        pools: list[dict],
        positions: list[int],
    ):
        param_sets = self._form_param_set(pools)
        responses = await self.client.eth_call(param_sets=param_sets)

        set_size = 2
        for idx, pos in enumerate(positions):
            start = idx * set_size
            end = start + set_size
            storage[pos] = responses[start:end]

        progress.update(task_id, advance=len(positions))

    def _form_param_set(self, pools: list[dict]):
        param_sets = []
        for pool in pools:
            for token in ["token0_address", "token1_address"]:
                param_set = [
                    {
                        "to": "0x" + pool[token].lower()[-40:],
                        "data": FUNCTION_HEX_SIGNATURES["erc20"]["balanceOf"][:10] + pool["pool_address"].lower().replace("0x", "").rjust(64, "0"),
                    }
                ]
                param_sets.append(param_set)

        return param_sets
