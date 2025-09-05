from src.exporters.manager import ExportManager
from src.fetchers.contract_fetcher import ContractFetcher
from src.utils.enumeration import EntityType


class AddressParser:
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
        account_addresses = []
        for raw_receipts in items:
            for raw_receipt in raw_receipts:
                contracts, accounts = self._parse(raw_receipt)
                contract_addresses.extend(contracts)
                account_addresses.extend(accounts)

        contract_addresses = list(set(contract_addresses))
        account_addresses = list(set(account_addresses))
        await self.client.run(
            contract_addresses, show_progress=show_progress
        )

        self.exporter.add_items(EntityType.ACCOUNT_ADDRESS, [account_addresses])

    def _parse(self, item: dict):
        contract_address = []
        account_address = []  # Externally Owned Account: wallet address, user_address, ...

        account_address.append(item.get("from"))
        if item["contractAddress"]:
            contract_address.append(item["contractAddress"])

        for log in item.get("logs", []):
            contract_address.append(log["address"])

        if item["to"] not in contract_address:
            account_address.append(item["to"])

        return contract_address, account_address
