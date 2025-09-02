from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.log import Log
from src.schemas.python.receipt import Receipt
from src.utils.enumeration import EntityType


class RawReceiptParser:
    def __init__(self, exporter, target=[EntityType.RECEIPT, EntityType.LOG]):
        self.exporter = exporter
        self.target = target

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
                "Receipt - Log: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                for raw_receipt in item:
                    if EntityType.RECEIPT in self.target:
                        receipt = self._parse_receipt(raw_receipt)
                        self.exporter.add_item(EntityType.RECEIPT, receipt.model_dump())

                    if EntityType.LOG in self.target:
                        for raw_log in raw_receipt.get("logs", []):
                            log = self._parse_log(raw_log)
                            self.exporter.add_item(EntityType.LOG, log.model_dump())

                progress.update(task, advance=batch_size)

    def _parse_receipt(self, item: dict):
        receipt = Receipt(
            block_hash=item.get("blockHash"),
            block_number=item.get("blockNumber"),
            contract_address=item.get("contractAddress"),
            cumulative_gas_used=item.get("cumulativeGasUsed"),
            from_address=item.get("from"),
            gas_used=item.get("gasUsed"),
            effective_gas_price=item.get("effectiveGasPrice"),
            log_count=len(item.get("logs", [])),
            logs_bloom=item.get("logsBloom"),
            status=item.get("status"),
            to_address=item.get("to"),
            transaction_hash=item.get("transactionHash"),
            transaction_index=item.get("transactionIndex"),
            type=item.get("type"),
        )

        return receipt

    def _parse_log(self, item: dict):
        log = Log(
            address=item.get("address"),
            topics=item.get("topics"),
            data=item.get("data"),
            block_hash=item.get("blockHash"),
            block_number=item.get("blockNumber"),
            block_timestamp=item.get("blockTimestamp"),
            transaction_hash=item.get("transactionHash"),
            transaction_index=item.get("transactionIndex"),
            log_index=item.get("logIndex"),
            removed=item.get("removed"),
        )

        return log
