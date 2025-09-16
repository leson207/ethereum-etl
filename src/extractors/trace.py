from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.trace import Trace


class TraceExtractor:
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
                "Trace: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            results = []
            for block in items:
                for trace in block:
                    trace = self._extract(trace)
                    results.append(trace.model_dump())
                
                progress.update(task, advance=batch_size)
            
            return results

    def _extract(self, item: dict):
        try:
            trace = Trace(
                action=item["action"],
                block_hash=item["blockHash"],
                block_number=item["blockNumber"],
                error=item.get("error"),
                result=item["result"],
                subtraces=item["subtraces"],
                trace_address=item["traceAddress"],
                transaction_hash=item.get("transactionHash"),
                transaction_position=item.get("transactionPosition"),
                type=item["type"],
            )
            return trace
        except:
            print(item)
            raise
        # trace = Trace(
        #     action=item["action"],
        #     block_hash=item["blockHash"],
        #     block_number=item["blockNumber"],
        #     error=item.get("error"),
        #     result=item["result"],
        #     subtraces=item["subtraces"],
        #     trace_address=item["traceAddress"],
        #     transaction_hash=item["transactionHash"],
        #     transaction_position=item["transactionPosition"],
        #     type=item["type"],
        # )

        return trace
