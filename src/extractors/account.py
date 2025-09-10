from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.exporters.manager import ExportManager
from src.schemas.python.account import Account
from src.utils.enumeration import EntityType


class AccountExtractor:
    def __init__(self, exporter: ExportManager):
        self.exporter = exporter

    def run(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
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
                "Account: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for block in items:
                for receipt in block:
                    accounts = self.extract(receipt)
                    accounts = [item.model_dump() for item in accounts]
                    self.exporter.add_items(EntityType.ACCOUNT, accounts)

                progress.update(task, advance=batch_size)

    def extract(self, item: dict):
        contract_addresses = []
        account_addresses = []  # Externally Owned Account: wallet address, user_address, ...

        account_addresses.append(item.get("from"))
        if item["contractAddress"]:
            contract_addresses.append(item["contractAddress"])

        for log in item.get("logs", []):
            contract_addresses.append(log["address"])

        if item["to"] not in contract_addresses:
            account_addresses.append(item["to"])

        account_addresses = [Account(address=address) for address in account_addresses if address]

        return account_addresses
