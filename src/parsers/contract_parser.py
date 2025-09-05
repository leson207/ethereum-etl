from src.exporters.manager import ExportManager
from src.fetchers.contract_fetcher import ContractFetcher


class ContractParser:
    def __init__(self, exporter: ExportManager):
        self.exporter = exporter
        self.client = ContractFetcher(exporter)

    async def parse(
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
                contracts = self._parse(receipt)
                contract_addresses.extend(contracts)

        contract_addresses = list(set(contract_addresses))
        await self.client.run(contract_addresses, show_progress=show_progress)

    def _parse(self, item: dict):
        contract_address = []

        if item["contractAddress"]:
            contract_address.append(item["contractAddress"])

        for log in item.get("logs", []):
            contract_address.append(log["address"])

        return contract_address
