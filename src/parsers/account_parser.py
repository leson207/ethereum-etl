from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.exporters.manager import ExportManager
from src.utils.enumeration import EntityType


class AccountParser:
    def __init__(self, exporter: ExportManager):
        self.exporter = exporter

    def parse(
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
                    accounts = self._parse(receipt)
                    self.exporter.add_items(EntityType.ACCOUNT_ADDRESS, accounts)

                progress.update(task, advance=batch_size)

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

        return account_address
