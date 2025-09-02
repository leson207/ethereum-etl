from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.trace import Trace
from src.utils.enumeration import EntityType


class RawTraceParser:
    def __init__(self, exporter, target=[EntityType.TRACE]):
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
                "Trace: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                if EntityType.TRACE in self.target:
                    for raw_trace in item:
                        trace = self._parse(raw_trace)
                        self.exporter.add_item(EntityType.TRACE, trace.model_dump())

                progress.update(task, advance=batch_size)

    def _parse(self, item: dict):
        trace = Trace(
            action=item["action"],
            block_hash=item["blockHash"],
            block_number=item["blockNumber"],
            error=item.get("error"),
            result=item["result"],
            subtraces=item["subtraces"],
            trace_address=item["traceAddress"],
            transaction_hash=item["transactionHash"],
            transaction_position=item["transactionPosition"],
            type=item["type"],
        )

        return trace
