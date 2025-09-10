import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.abis.function import FUNCTION_HEX_SIGNATURES
from src.clients.rpc_client import RpcClient
from src.schemas.python.pool import Pool
from src.utils.enumeration import EntityType


class PoolExtractor:
    def __init__(self, exporter, client: RpcClient):
        self.exporter = exporter
        self.client = client

    async def run(
        self,
        contract_addresses,
        initial=None,
        total=None,
        batch_size=10,
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
                description="Pool: ",
                total=(total or len(contract_addresses)),
                completed=(initial or 0),
            )

            tasks = []
            for i in range(0, len(contract_addresses), batch_size):
                batch = contract_addresses[i : i + batch_size]
                atask = asyncio.create_task(
                    self._run(progress, task, batch, len(batch))
                )
                tasks.append(atask)

            for coro in asyncio.as_completed(tasks):
                result = await coro

                result = [i.model_dump() for i in result]
                self.exporter.add_items(EntityType.POOL, result)

    async def _run(self, progress, task, input, input_size):
        calls = self._form_call(input)
        responses = await self.client.send(calls)
        res = self.extract(calls, responses)
        progress.update(task, advance=input_size)
        return res

    def _form_call(self, contract_addresses):
        calls = []
        for contract_address in contract_addresses:
            for func in ["token0", "token1"]:
                call = (
                    "eth_call",
                    [
                        {
                            "to": contract_address.lower()[:42],
                            "data": FUNCTION_HEX_SIGNATURES["erc20"][func],
                        }
                    ],
                )
                calls.append(call)

        return calls

    def extract(self, calls, responses):
        pools = []
        for i in range(0, len(responses), 2):
            call = calls[i]
            token0, token1 = responses[i : i + 2]
            if "error" in token0:
                continue
            
            pool = Pool(
                pool_address=call[1][0]["to"],
                token0_address=token0["result"],
                token1_address=token1["result"]
            )
            pools.append(pool)
        return pools
