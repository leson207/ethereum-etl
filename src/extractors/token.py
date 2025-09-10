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
from clients.rpc_client import RpcClient
from src.schemas.python.token import Token
from src.utils.enumeration import EntityType


class TokenExtractor:
    def __init__(self, exporter, client: RpcClient):
        self.exporter = exporter
        self.client = client

    async def run(
        self,
        contract_addresses,
        initial=None,
        total=None,
        batch_size=5,
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
                description="Token: ",
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
                self.exporter.add_items(EntityType.TOKEN, result)

    async def _run(self, progress, task, input, input_size):
        param_sets = self._form_param_set(input)
        responses = await self.client.eth_call(param_sets=param_sets)
        res = self.extract(param_sets, responses)
        progress.update(task, advance=input_size)
        return res

    def _form_param_set(self, contract_addresses):
        param_sets = []
        for contract_address in contract_addresses:
            for func in ["name", "symbol", "decimals", "totalSupply"]:
                param_set = [
                        {
                            "to": "0x"+contract_address.lower()[-40:],
                            "data": FUNCTION_HEX_SIGNATURES["erc20"][func],
                        }
                    ]
                param_sets.append(param_set)

        return param_sets

    def extract(self, param_sets, responses):
        tokens = []
        for i in range(0, len(responses), 4):
            param_set = param_sets[i]
            name, symbol, decimals, total_suplly = responses[i : i + 4]
            if "error" in name:
                continue

            # token = Token(
            #     address=param_set[0]["to"],
            #     name=name["result"],
            #     symbol=symbol["result"],
            #     decimals=decimals["result"],
            #     total_suplly=total_suplly["result"],
            # )
            # tokens.append(token)
            try:
                token = Token(
                    address=param_set[0]["to"],
                    name=name["result"],
                    symbol=symbol["result"],
                    decimals=decimals["result"],
                    total_suplly=total_suplly["result"],
                )
                tokens.append(token)
            except Exception:
                print(param_set[0]["to"])
                continue

        return tokens