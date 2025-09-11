from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.log import Log


class LogExtractor:
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
                "Log: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            results = []
            for block in items:
                for receipt in block:
                    for log in receipt["logs"]:
                        log = self.extract(log)
                        results.append(log.model_dump())

                progress.update(task, advance=batch_size)
            
            return results

    def extract(self, item: dict):
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
