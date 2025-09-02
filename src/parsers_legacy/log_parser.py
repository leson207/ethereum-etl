from rich.progress import Progress

from src.schemas.python.log import Log
from src.utils.enumeration import EntityType


class LogParser:
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
                "Log: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                log = self._parse(item)
                self.exporter.add_item(EntityType.LOG, [log.model_dump()])
                progress.update(task, advance=batch_size)

    def _parse(self, item: dict):
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
