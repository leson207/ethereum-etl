from rich.progress import Progress

from src.schemas.python.receipt import Receipt
from src.utils.enumeration import EntityType


class ReceiptParser:
    def __init__(self, exporter):
        self.exporter = exporter

    def parse(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        with Progress(disable=not show_progress) as progress:
            task = progress.add_task(
                "Receipt: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                receipt = self._parse(item)
                self.exporter.add_item(EntityType.RECEIPT, [receipt.model_dump()])
                progress.update(task, advance=batch_size)

    def _parse(self, item: dict):
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
