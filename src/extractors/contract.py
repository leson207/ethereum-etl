import asyncio

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.clients.etherscan_client import EtherscanClient
from src.exporters.manager import ExportManager
from src.schemas.python.contract import Contract
from src.utils.enumeration import EntityType


# TODO: separate collect here
class ContractExtractor:
    def __init__(self, exporter: ExportManager, client: EtherscanClient):
        self.exporter = exporter
        self.client = client

    async def run(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        contract_addresses = []
        for block in items:
            for receipt in block:
                contracts = self.collect(receipt)
                contract_addresses.extend(contracts)

        contract_addresses = list(set(contract_addresses))

        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                description="Contract: ",
                total=len(contract_addresses),
                completed=0,
            )

            tasks = []
            for contract_address in contract_addresses:
                atask = asyncio.create_task(
                    self._run(progress, task, contract_address, 1)
                )
                tasks.append(atask)

            for coro in asyncio.as_completed(tasks):
                result = await coro
                if result is None:
                    continue

                self.exporter.add_item(EntityType.CONTRACT_ADDRESS, result.model_dump())

    async def _run(self, progress, task, input, input_size):
        res = await self.extract(input)
        progress.update(task, advance=input_size)
        return res

    def collect(self, item: dict):
        contract_address = []

        if item["contractAddress"]:
            contract_address.append(item["contractAddress"])

        for log in item.get("logs", []):
            contract_address.append(log["address"])

        return contract_address

    async def extract(self, contract_address):
        result = await self.client.get_contract_source_code(contract_address)
        if not result or result == "Invalid Address format":
            return None

        
        # contract = Contract(
        #     name=result["ContractName"],
        #     address=contract_address,
        #     abi=result["ABI"],
        # )

        try:
            contract = Contract(
                name=result["ContractName"],
                address=contract_address,
                abi=result["ABI"],
            )
        except Exception:
            print(result)
            raise

        return contract
