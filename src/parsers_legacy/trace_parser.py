from rich.progress import Progress

from src.schemas.python.trace import Trace
from src.utils.enumeration import EntityType


class TraceParser:
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
                "Trace: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                for raw_trace in item:
                    trace = self._parse(raw_trace)
                    self.exporter.add_item(EntityType.TRACE, [trace.model_dump()])
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
